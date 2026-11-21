package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/hop-/cachydb/internal/auth"
	"github.com/hop-/cachydb/internal/config"
	"github.com/spf13/cobra"
)

// healthcheckTimeout bounds the whole probe (connect, request, response).
const healthcheckTimeout = 3 * time.Second

var healthcheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Check that a running HTTP server is healthy (exit 0) or not (exit 1)",
	Long: `Probe the /healthz endpoint of a cachydb server running with the HTTP
transport, like pg_isready for PostgreSQL. Exit status 0 means the server
answered 200 (the engine is open); 1 means it did not answer, answered
another status (503 while it shuts down), or took longer than 3 seconds.
The reason goes to stderr; nothing is written on success.

The URL defaults to http://127.0.0.1:<port>/healthz, with the port from
-p/--port, then PORT, then 7601; https:// when the server's TLS
certificate and key are set (--tls-cert and --tls-key, then TLS_CERT and
TLS_KEY). Without --url, if the transport (-t, then TRANSPORT) is stdio
there is no HTTP server to probe: healthcheck prints a note to stderr and
exits 0, so a stdio container is not marked unhealthy. An explicit --url
is always probed. An HTTPS URL on a loopback host (127.0.0.1, ::1,
localhost) is probed without verifying the certificate; /healthz needs
no token.

healthcheck never opens the database, so it works while a server runs. The
official Docker image uses it as its HEALTHCHECK; see "cachydb docs mcp".`,
	Example: `  cachydb healthcheck --url http://127.0.0.1:7601/healthz   # probe one endpoint
  TRANSPORT=http PORT=8080 cachydb healthcheck            # local server settings from the environment
  cachydb healthcheck -t http -p 7601                     # the same with flags`,
	Args: cobra.NoArgs,
	RunE: runHealthcheck,
}

var (
	healthcheckURL       string
	healthcheckPort      int
	healthcheckTransport string
	healthcheckTLSCert   string
	healthcheckTLSKey    string
)

func init() {
	c := config.GetConfig()
	healthcheckCmd.Flags().StringVar(
		&healthcheckURL,
		"url", "",
		"health endpoint to probe (default http://127.0.0.1:<port>/healthz)",
	)
	healthcheckCmd.Flags().IntVarP(
		&healthcheckPort,
		"port", "p",
		c.Port,
		"port of the local server, used when --url is not given (env PORT)",
	)
	healthcheckCmd.Flags().StringVarP(
		&healthcheckTransport,
		"transport", "t",
		c.Transport,
		"transport of the local server; stdio means nothing to probe (env TRANSPORT)",
	)
	healthcheckCmd.Flags().StringVar(
		&healthcheckTLSCert,
		"tls-cert",
		c.TLSCert,
		"TLS certificate of the local server; with --tls-key the default URL is https (env TLS_CERT)",
	)
	healthcheckCmd.Flags().StringVar(
		&healthcheckTLSKey,
		"tls-key",
		c.TLSKey,
		"TLS key of the local server; with --tls-cert the default URL is https (env TLS_KEY)",
	)
	rootCmd.AddCommand(healthcheckCmd)
}

func runHealthcheck(cmd *cobra.Command, _ []string) error {
	cmd.SilenceUsage = true
	ctx, cancel := context.WithTimeout(cmd.Context(), healthcheckTimeout)
	defer cancel()
	return healthcheck(ctx, cmd.ErrOrStderr(), healthcheckURL, healthcheckTransport, healthcheckPort)
}

// healthcheck probes url, or the local server's /healthz when url is
// empty, and returns nil only for HTTP 200. With an empty url and the
// stdio transport there is nothing to probe: it writes a note to stderr
// and returns nil. The default URL is https when the --tls-cert and
// --tls-key flags (defaults TLS_CERT, TLS_KEY) are both set. ctx bounds
// the request.
func healthcheck(ctx context.Context, stderr io.Writer, url, transport string, port int) error {
	if url == "" {
		if err := validateTransport(transport); err != nil {
			return fmt.Errorf("healthcheck: %w", err)
		}
		if transport == "stdio" {
			_, err := fmt.Fprintln(stderr, "healthcheck: transport is stdio; nothing to probe")
			return err
		}
		scheme := "http"
		if healthcheckTLSCert != "" && healthcheckTLSKey != "" {
			scheme = "https"
		}
		url = fmt.Sprintf("%s://127.0.0.1:%d/healthz", scheme, port)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp, err := healthClient(req.URL).Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()
	// Drain a little of the body so the connection closes cleanly; the
	// content does not matter, only the status does.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: GET %s: unhealthy: %s", url, resp.Status)
	}
	return nil
}

// healthClient returns the client that probes u. An HTTPS URL on a
// loopback host skips certificate verification: the certificate names
// the server's public host, not 127.0.0.1, and often is self-signed, and
// the probe sends no secret and only reads the status.
func healthClient(u *url.URL) *http.Client {
	if u.Scheme != "https" || !auth.IsLoopback(u.Hostname()) {
		return http.DefaultClient
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	return &http.Client{Transport: t}
}
