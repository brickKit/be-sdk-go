package rpc

import (
	"encoding/json"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The retry numbers of P7.8, written exactly as the spec's service config.
type retryPolicy struct {
	MaxAttempts          int      `json:"maxAttempts"`
	InitialBackoff       string   `json:"initialBackoff"`
	MaxBackoff           string   `json:"maxBackoff"`
	BackoffMultiplier    float64  `json:"backoffMultiplier"`
	RetryableStatusCodes []string `json:"retryableStatusCodes"`
}

type methodName struct {
	Service string `json:"service"`
	Method  string `json:"method"`
}

type methodConfig struct {
	Name        []methodName `json:"name"`
	RetryPolicy retryPolicy  `json:"retryPolicy"`
}

type retryThrottling struct {
	MaxTokens  float64 `json:"maxTokens"`
	TokenRatio float64 `json:"tokenRatio"`
}

type serviceConfig struct {
	MethodConfig    []methodConfig  `json:"methodConfig"`
	RetryThrottling retryThrottling `json:"retryThrottling"`
}

// DefaultServiceConfig is the gRPC service config of P7.8 for every service in files (normally
// protoregistry.GlobalFiles, where generated code registers itself): one methodConfig entry naming
// each method whose idempotency_level is NO_SIDE_EFFECTS or IDEMPOTENT, sorted by service and
// method, with the spec's retryPolicy (3 attempts in total, 0.05s, 0.5s, ×2, UNAVAILABLE), plus
// retryThrottling {10, 0.1}. Without a retryable method, methodConfig is empty and the throttling
// stays. Other methods get only gRPC's transparent retry.
func DefaultServiceConfig(files *protoregistry.Files) (string, error) {
	names := retryableMethods(files)
	sc := serviceConfig{
		MethodConfig:    []methodConfig{},
		RetryThrottling: retryThrottling{MaxTokens: 10, TokenRatio: 0.1},
	}
	if len(names) > 0 {
		sc.MethodConfig = append(sc.MethodConfig, methodConfig{Name: names, RetryPolicy: retryPolicy{
			MaxAttempts: 3, InitialBackoff: "0.05s", MaxBackoff: "0.5s", BackoffMultiplier: 2,
			RetryableStatusCodes: []string{"UNAVAILABLE"},
		}})
	}
	b, err := json.Marshal(sc)
	return string(b), err
}

func retryableMethods(files *protoregistry.Files) []methodName {
	var names []methodName
	files.RangeFiles(func(f protoreflect.FileDescriptor) bool {
		for i := 0; i < f.Services().Len(); i++ {
			sd := f.Services().Get(i)
			for j := 0; j < sd.Methods().Len(); j++ {
				md := sd.Methods().Get(j)
				if retryable(md) {
					names = append(names, methodName{Service: string(sd.FullName()), Method: string(md.Name())})
				}
			}
		}
		return true
	})
	sort.Slice(names, func(a, b int) bool {
		if names[a].Service != names[b].Service {
			return names[a].Service < names[b].Service
		}
		return names[a].Method < names[b].Method
	})
	return names
}

// retryable reports whether a method's contract allows retries (P7.8).
func retryable(md protoreflect.MethodDescriptor) bool {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return false
	}
	switch opts.GetIdempotencyLevel() {
	case descriptorpb.MethodOptions_NO_SIDE_EFFECTS, descriptorpb.MethodOptions_IDEMPOTENT:
		return true
	}
	return false
}
