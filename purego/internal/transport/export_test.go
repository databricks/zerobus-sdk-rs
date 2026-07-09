package transport

import "google.golang.org/grpc/credentials/insecure"

// WithInsecure disables transport security. Auth tokens ride gRPC metadata, so
// an insecure connection would send bearer tokens in plaintext; it lives in a
// _test.go file so production code can't reach it. Tests use it for in-memory
// and local plaintext endpoints.
func WithInsecure() DialOption {
	return func(cfg *dialConfig) { cfg.creds = insecure.NewCredentials() }
}
