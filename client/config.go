// Package client provides authenticated HTTP requests and resumable WebSocket
// streams for an Outpost relay.
package client

import (
	"fmt"
	"net/url"
	"strings"
)

// Config identifies a relay and the device credential used to access it.
// URL may include a reverse-proxy path prefix. Configuration loading and
// credential storage are the caller's responsibility.
type Config struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// Validate checks that URL is an HTTP(S) base URL without credentials, a query,
// or a fragment, and that Token is nonempty and contains no whitespace.
func (c Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required")
	}
	if c.Token == "" {
		return fmt.Errorf("token is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("url must be an http(s) base URL without credentials, query or fragment")
	}
	if strings.ContainsAny(c.Token, " \t\r\n") {
		return fmt.Errorf("token must not contain whitespace")
	}
	return nil
}
