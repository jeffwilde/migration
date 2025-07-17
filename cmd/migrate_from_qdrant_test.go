package cmd

import (
	"net/url"
	"strings"
	"testing"
)

func Test_getPort(t *testing.T) {
	tests := []struct {
		name     string
		url      *url.URL
		expected int
	}{
		{
			name:     "tls enabled, custom port",
			url:      &url.URL{Scheme: "https", Host: "localhost:6334"},
			expected: 6334,
		},
		{
			name:     "tls enabled, default port",
			url:      &url.URL{Scheme: "https", Host: "localhost"},
			expected: 6334,
		},
		{
			name:     "tls disabled, default port",
			url:      &url.URL{Scheme: "http", Host: "localhost"},
			expected: 6334,
		},
		{
			name:     "tls disabled, custom port",
			url:      &url.URL{Scheme: "http", Host: "localhost:6334"},
			expected: 6334,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := getPort(tt.url)
			if got != tt.expected {
				t.Errorf("getPort() got = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func Test_parseQdrantUrl(t *testing.T) {
	tests := []struct {
		name        string
		urlStr      string
		expectedHost string
		expectedPort int
		expectedTLS  bool
		expectError  bool
		errorContains string
	}{
		{
			name:        "valid GRPC URL with port",
			urlStr:      "http://localhost:6334",
			expectedHost: "localhost",
			expectedPort: 6334,
			expectedTLS:  false,
			expectError:  false,
		},
		{
			name:        "valid HTTPS GRPC URL with port",
			urlStr:      "https://cluster.qdrant.com:6334",
			expectedHost: "cluster.qdrant.com",
			expectedPort: 6334,
			expectedTLS:  true,
			expectError:  false,
		},
		{
			name:        "HTTP URL without port defaults to GRPC port",
			urlStr:      "http://localhost",
			expectedHost: "localhost",
			expectedPort: 6334,
			expectedTLS:  false,
			expectError:  false,
		},
		{
			name:        "HTTPS URL without port defaults to GRPC port",
			urlStr:      "https://cluster.qdrant.com",
			expectedHost: "cluster.qdrant.com",
			expectedPort: 6334,
			expectedTLS:  true,
			expectError:  false,
		},
		{
			name:        "HTTPS URL with explicit port 443 shows warning",
			urlStr:      "https://cluster.qdrant.com:443",
			expectedHost: "cluster.qdrant.com",
			expectedPort: 443,
			expectedTLS:  true,
			expectError:  false,
		},
		{
			name:        "REST port should warn but not fail",
			urlStr:      "http://localhost:6333",
			expectedHost: "localhost",
			expectedPort: 6333,
			expectedTLS:  false,
			expectError:  false,
		},
		{
			name:        "HTTPS REST port should warn but not fail",
			urlStr:      "https://cluster.qdrant.com:6333",
			expectedHost: "cluster.qdrant.com",
			expectedPort: 6333,
			expectedTLS:  true,
			expectError:  false,
		},
		{
			name:        "invalid port should fail",
			urlStr:      "http://localhost:invalid",
			expectError:  true,
			errorContains: "invalid port",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, tls, err := parseQdrantUrl(tt.urlStr)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("parseQdrantUrl() expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("parseQdrantUrl() error = %v, expected to contain %v", err, tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Errorf("parseQdrantUrl() unexpected error = %v", err)
				return
			}

			if host != tt.expectedHost {
				t.Errorf("parseQdrantUrl() host = %v, expected %v", host, tt.expectedHost)
			}
			if port != tt.expectedPort {
				t.Errorf("parseQdrantUrl() port = %v, expected %v", port, tt.expectedPort)
			}
			if tls != tt.expectedTLS {
				t.Errorf("parseQdrantUrl() tls = %v, expected %v", tls, tt.expectedTLS)
			}
		})
	}
}

func Test_validateQdrantPort(t *testing.T) {
	tests := []struct {
		name        string
		urlStr      string
		port        int
		expectError bool
		errorContains string
	}{
		{
			name:        "valid GRPC port",
			urlStr:      "http://localhost:6334",
			port:        6334,
			expectError: false,
		},
		{
			name:        "REST port should warn but not fail",
			urlStr:      "http://localhost:6333",
			port:        6333,
			expectError: false,
		},
		{
			name:        "custom port should pass",
			urlStr:      "http://localhost:8080",
			port:        8080,
			expectError: false,
		},
		{
			name:        "HTTPS default port should pass but warn",
			urlStr:      "https://cluster.qdrant.com",
			port:        443,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedUrl, _ := url.Parse(tt.urlStr)
			err := validateQdrantPort(parsedUrl, tt.port)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("validateQdrantPort() expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("validateQdrantPort() error = %v, expected to contain %v", err, tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Errorf("validateQdrantPort() unexpected error = %v", err)
			}
		})
	}
}

func Test_probeEndpointType(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		port        int
		useTLS      bool
		expectRest  bool
		expectError bool
	}{
		{
			name:        "non-existent endpoint should return error",
			host:        "non-existent-host-12345",
			port:        6334,
			useTLS:      false,
			expectRest:  false,
			expectError: true,
		},
		{
			name:        "localhost with invalid port should return error",
			host:        "localhost",
			port:        99999,
			useTLS:      false,
			expectRest:  false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isRest, err := probeEndpointType(tt.host, tt.port, tt.useTLS, "")
			
			if tt.expectError && err == nil {
				t.Errorf("probeEndpointType() expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("probeEndpointType() unexpected error: %v", err)
			}
			if isRest != tt.expectRest {
				t.Errorf("probeEndpointType() isRest = %v, expected %v", isRest, tt.expectRest)
			}
		})
	}
}
