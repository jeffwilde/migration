package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/pterm/pterm"
	"google.golang.org/grpc"

	"github.com/qdrant/go-client/qdrant"
)

const (
	HTTPS                = "https"
	QDRANT_REST_PORT     = 6333
	QDRANT_GRPC_PORT     = 6334
	HTTPS_DEFAULT_PORT   = 443
	HTTP_DEFAULT_PORT    = 80
)

func connectToQdrant(globals *Globals, host string, port int, apiKey string, useTLS bool, maxMessageSize int) (*qdrant.Client, error) {
	// If this looks like a REST port, probe the endpoint to verify
	if port == QDRANT_REST_PORT {
		pterm.Info.Println("Probing endpoint to verify protocol type...")
		isRest, probeErr := probeEndpointType(host, port, useTLS, apiKey)
		if probeErr != nil && isRest {
			return nil, probeErr
		}
		if isRest {
			return nil, fmt.Errorf("confirmed: endpoint is serving REST API, but this tool requires GRPC")
		}
		pterm.Info.Println("Endpoint probe successful - proceeding with GRPC connection")
	}
	debugLogger := logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		pterm.Debug.Printf(msg, fields...)
	})

	var grpcOptions []grpc.DialOption

	if globals.Trace {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall, logging.PayloadSent, logging.PayloadReceived)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}
	if globals.Debug {
		pterm.EnableDebugMessages()
		loggingOptions := logging.WithLogOnEvents(logging.StartCall, logging.FinishCall)
		grpcOptions = append(grpcOptions, grpc.WithChainUnaryInterceptor(logging.UnaryClientInterceptor(debugLogger, loggingOptions)))
		grpcOptions = append(grpcOptions, grpc.WithChainStreamInterceptor(logging.StreamClientInterceptor(debugLogger, loggingOptions)))
	}

	if maxMessageSize != 0 {
		grpcOptions = append(grpcOptions, grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSize),
		))
	}

	tlsConfig := tls.Config{
		InsecureSkipVerify: globals.SkipTlsVerification,
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 apiKey,
		UseTLS:                 useTLS,
		TLSConfig:              &tlsConfig,
		GrpcOptions:            grpcOptions,
		SkipCompatibilityCheck: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return client, nil
}

func getPort(u *url.URL) (int, error) {
	if u.Port() != "" {
		sourcePort, err := strconv.Atoi(u.Port())
		if err != nil {
			return 0, fmt.Errorf("failed to parse port: %w", err)
		}
		return sourcePort, nil
	}
	
	// Since this tool only uses GRPC, default to GRPC port regardless of scheme
	// This is more user-friendly than defaulting to HTTP/HTTPS ports
	pterm.Info.Printfln("No port specified, defaulting to GRPC port %d", QDRANT_GRPC_PORT)
	return QDRANT_GRPC_PORT, nil
}

func parseQdrantUrl(urlStr string) (host string, port int, tls bool, err error) {
	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to parse URL: %w", err)
	}

	host = parsedUrl.Hostname()
	tls = parsedUrl.Scheme == HTTPS
	port, err = getPort(parsedUrl)
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to parse port: %w", err)
	}

	// Validate and provide user-friendly warnings for common port issues
	err = validateQdrantPort(parsedUrl, port)
	if err != nil {
		return "", 0, false, err
	}

	return host, port, tls, nil
}

func validateQdrantPort(parsedUrl *url.URL, port int) error {
	// Warn if user provided REST port when GRPC is expected
	if port == QDRANT_REST_PORT {
		pterm.Warning.Printfln("Detected port %d which is typically used for Qdrant REST API", QDRANT_REST_PORT)
		pterm.Warning.Printfln("This tool uses GRPC and typically expects port %d. Will attempt connection and verify endpoint type", QDRANT_GRPC_PORT)
	}

	// Warn about HTTPS with port 443 (likely REST endpoint)
	if parsedUrl.Scheme == HTTPS && port == HTTPS_DEFAULT_PORT {
		pterm.Warning.Printfln("Using HTTPS with port %d. Qdrant cloud instances typically serve REST on port %d and GRPC on port %d", HTTPS_DEFAULT_PORT, HTTPS_DEFAULT_PORT, QDRANT_GRPC_PORT)
		pterm.Warning.Printfln("If you encounter connection issues, try specifying the GRPC port explicitly: %s:%d", parsedUrl.Host, QDRANT_GRPC_PORT)
	}

	return nil
}

// probeEndpointType attempts to determine if an endpoint is REST or GRPC
func probeEndpointType(host string, port int, useTLS bool, apiKey string) (isRest bool, err error) {
	// First try a simple GRPC health check
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to create a GRPC client and make a simple call
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:                   host,
		Port:                   port,
		APIKey:                 apiKey,
		UseTLS:                 useTLS,
		TLSConfig:              &tls.Config{InsecureSkipVerify: true},
		SkipCompatibilityCheck: true,
	})
	if err == nil {
		// Try a simple GRPC call - list collections
		_, grpcErr := client.ListCollections(ctx)
		if grpcErr == nil {
			return false, nil // Successfully made GRPC call
		}
		// If GRPC call failed, check if it's a protocol-level error that suggests REST endpoint
		if strings.Contains(grpcErr.Error(), "malformed header") || 
		   strings.Contains(grpcErr.Error(), "transport: received the unexpected content-type") ||
		   strings.Contains(grpcErr.Error(), "http2: server sent GOAWAY") {
			// These errors suggest we're talking to a REST endpoint
			return true, fmt.Errorf("endpoint appears to be serving REST API, but GRPC is required")
		}
	}

	// If GRPC client creation failed or gave ambiguous errors, try REST probe
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	
	restURL := fmt.Sprintf("%s://%s:%d/collections", scheme, host, port)
	req, err := http.NewRequestWithContext(ctx, "GET", restURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create REST probe request: %w", err)
	}
	
	if apiKey != "" {
		req.Header.Set("api-key", apiKey)
	}
	
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	
	resp, err := httpClient.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 || resp.StatusCode == 401 || resp.StatusCode == 403 {
			// Got a valid HTTP response from /collections endpoint
			return true, fmt.Errorf("endpoint is serving REST API on /collections, but this tool requires GRPC")
		}
	}
	
	// If both probes failed, return the original error (connection issue, etc.)
	return false, fmt.Errorf("unable to determine endpoint type - connection may be failing")
}

func validateBatchSize(batchSize int) error {
	if batchSize < 1 {
		return fmt.Errorf("batch size must be greater than 0")
	}
	return nil
}

func displayMigrationStart(sourceProvider, sourceCollection, targetCollection string) {
	pterm.DefaultSection.Println("Starting Migration To Qdrant")

	from := fmt.Sprintf("%s@%s", sourceCollection, sourceProvider)
	to := fmt.Sprintf("%s@qdrant", targetCollection)

	table := pterm.TableData{
		{pterm.FgLightCyan.Sprint("From → To:"), pterm.FgLightGreen.Sprintf("%s  →  %s", from, to)},
	}

	_ = pterm.DefaultTable.
		WithHasHeader(false).
		WithBoxed(true).
		WithData(table).
		Render()

	pterm.Println()
}

func displayMigrationProgress(bar *pterm.ProgressbarPrinter, offsetCount uint64) {
	if offsetCount > 0 {
		pterm.Info.Printfln("Starting from offset %d", offsetCount)
		bar.Add(int(offsetCount))
	} else {
		pterm.Info.Printfln("Starting from the beginning")
	}
	fmt.Print("\n")
}

func arbitraryIDToUUID(id string) *qdrant.PointId {
	// If already a valid UUID, use it directly
	if _, err := uuid.Parse(id); err == nil {
		return qdrant.NewIDUUID(id)
	}

	// Otherwise create a deterministic UUID based on the ID
	deterministicUUID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(id))
	return qdrant.NewIDUUID(deterministicUUID.String())
}
