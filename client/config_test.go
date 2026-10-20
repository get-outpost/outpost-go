package client_test

import (
	"strings"
	"testing"

	"github.com/get-outpost/outpost-go/client"
)

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  client.Config
		want string
	}{
		{"http", client.Config{URL: "http://localhost:8080", Token: "op_fixture"}, ""},
		{"https_prefix", client.Config{URL: "https://relay.example/proxy/path/", Token: "op_fixture"}, ""},
		{"missing_url", client.Config{Token: "op_fixture"}, "url is required"},
		{"missing_token", client.Config{URL: "https://relay.example"}, "token is required"},
		{"relative_url", client.Config{URL: "/relay", Token: "op_fixture"}, "url must"},
		{"websocket_url", client.Config{URL: "wss://relay.example", Token: "op_fixture"}, "url must"},
		{"missing_host", client.Config{URL: "http:///relay", Token: "op_fixture"}, "url must"},
		{"credentials", client.Config{URL: "https://user:pass@relay.example", Token: "op_fixture"}, "url must"},
		{"query", client.Config{URL: "https://relay.example?after=1", Token: "op_fixture"}, "url must"},
		{"empty_query", client.Config{URL: "https://relay.example?", Token: "op_fixture"}, ""},
		{"fragment", client.Config{URL: "https://relay.example#section", Token: "op_fixture"}, "url must"},
		{"bad_escape", client.Config{URL: "https://relay.example/%zz", Token: "op_fixture"}, "url must"},
		{"token_space", client.Config{URL: "https://relay.example", Token: "op fixture"}, "token must"},
		{"token_tab", client.Config{URL: "https://relay.example", Token: "op\tfixture"}, "token must"},
		{"token_newline", client.Config{URL: "https://relay.example", Token: "op\r\nfixture"}, "token must"},
	}
	// Explicit credentials must not depend on login state or environment files.
	t.Setenv("OUTPOST_URL", "invalid")
	t.Setenv("OUTPOST_TOKEN", "invalid token")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			_, newErr := client.New(tt.cfg)
			if tt.want == "" {
				if err != nil || newErr != nil {
					t.Fatalf("valid config rejected: %v, New: %v", err, newErr)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) || newErr == nil {
				t.Fatalf("Validate = %v, New = %v, want %q", err, newErr, tt.want)
			}
			if strings.Contains(err.Error(), "login") || strings.Contains(err.Error(), "OUTPOST_") {
				t.Fatalf("SDK validation depends on CLI configuration: %v", err)
			}
		})
	}
}
