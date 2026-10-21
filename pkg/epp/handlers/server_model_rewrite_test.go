/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlers

import (
	"context"
	"strings"
	"testing"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
)

// modelRewriteDirector serves every request under target, as a model rewrite does.
type modelRewriteDirector struct {
	mockDirector
	target string
}

func (d *modelRewriteDirector) HandleRequest(_ context.Context, reqCtx *RequestContext, body *fwkrh.InferenceRequestBody) (*RequestContext, error) {
	reqCtx.IncomingModelName = body.Model
	reqCtx.TargetModelName = d.target
	return reqCtx, nil
}

func TestProcessRewritesStreamedModelNameAcrossChunks(t *testing.T) {
	const (
		incoming = "client-model"
		target   = "internal-model"
	)
	stream := `data: {"id":"c","model":"` + target + `","choices":[{"index":0,"delta":{"content":"Hi"}}]}` + "\n\n" +
		`data: {"id":"c","model":"` + target + `","choices":[{"index":0,"delta":{"content":"!"}}]}` + "\n\n" +
		"data: [DONE]\n\n"

	// Every cut that lands inside a model field leaves neither chunk with the whole name.
	for cut := 1; cut < len(stream); cut++ {
		srv := &replayProcessServer{
			ctx: log.IntoContext(context.Background(), logr.Discard()),
			reqs: []*extProcPb.ProcessingRequest{
				newRequestHeaders(map[string]string{":path": "/v1/chat/completions"}),
				{Request: &extProcPb.ProcessingRequest_RequestBody{RequestBody: &extProcPb.HttpBody{
					Body:        []byte(`{"model":"` + incoming + `","messages":[{"role":"user","content":"Hi"}],"stream":true}`),
					EndOfStream: true,
				}}},
				{Request: &extProcPb.ProcessingRequest_ResponseHeaders{ResponseHeaders: &extProcPb.HttpHeaders{
					Headers: &configPb.HeaderMap{Headers: []*configPb.HeaderValue{
						{Key: ":status", RawValue: []byte("200")},
						{Key: "content-type", RawValue: []byte("text/event-stream")},
					}},
				}}},
				{Request: &extProcPb.ProcessingRequest_ResponseBody{ResponseBody: &extProcPb.HttpBody{Body: []byte(stream[:cut])}}},
				{Request: &extProcPb.ProcessingRequest_ResponseBody{ResponseBody: &extProcPb.HttpBody{Body: []byte(stream[cut:]), EndOfStream: true}}},
			},
		}
		registry := NewParserRegistry([]fwkrh.Parser{openai.NewOpenAIParser()}, logr.Discard())
		require.NoError(t, NewStreamingServer(nil, &modelRewriteDirector{target: target}, registry, 0).Process(srv))

		var client strings.Builder
		for _, resp := range srv.sentResponses {
			client.Write(resp.GetResponseBody().GetResponse().GetBodyMutation().GetStreamedResponse().GetBody())
		}
		require.Equal(t, strings.ReplaceAll(stream, target, incoming), client.String(), "cut at byte %d", cut)
	}
}
