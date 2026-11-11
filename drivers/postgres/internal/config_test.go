package driver

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		expectErr        bool
		expectedContains []string
	}{
		{
			name: "valid config with ssl disabled",
			config: &Config{
				Host:     "localhost",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeDisable,
				},
			},
			expectErr: false,
			expectedContains: []string{
				"sslmode=disable",
				"localhost:5432",
				"/postgres",
			},
		},
		{
			name: "valid config with ssl require",
			config: &Config{
				Host:     "db.internal",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeRequire,
				},
			},
			expectErr: false,
			expectedContains: []string{
				"sslmode=require",
			},
		},
		{
			name: "valid config with jdbc params",
			config: &Config{
				Host:     "db.internal",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
				JDBCURLParams: map[string]string{
					"connect_timeout":  "10",
					"application_name": "olake",
				},
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeDisable,
				},
			},
			expectErr: false,
			expectedContains: []string{
				"connect_timeout=10",
				"application_name=olake",
			},
		},
		{
			name: "invalid host with scheme",
			config: &Config{
				Host:     "https://localhost",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
			},
			expectErr: true,
		},
		{
			name: "invalid port out of range",
			config: &Config{
				Host:     "localhost",
				Port:     70000,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
			},
			expectErr: true,
		},
		{
			name: "invalid verify-ca without server ca",
			config: &Config{
				Host:     "localhost",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "postgres",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeVerifyCA,
				},
			},
			expectErr: true,
		},
		{
			name: "invalid missing database",
			config: &Config{
				Host:     "localhost",
				Port:     5432,
				Username: "postgres",
				Password: "secret",
				Database: "",
			},
			expectErr: true,
		},
		{
			name: "invalid negative max threads",
			config: &Config{
				Host:       "localhost",
				Port:       5432,
				Username:   "postgres",
				Password:   "secret",
				Database:   "postgres",
				MaxThreads: -1,
			},
			expectErr: true,
		},
		{
			name: "invalid negative retry count",
			config: &Config{
				Host:       "localhost",
				Port:       5432,
				Username:   "postgres",
				Password:   "secret",
				Database:   "postgres",
				RetryCount: -1,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
			if tt.config.Connection == nil {
				t.Fatalf("expected connection URL to be built")
			}

			uri := tt.config.Connection.String()
			for _, expected := range tt.expectedContains {
				if !strings.Contains(uri, expected) {
					t.Fatalf("expected connection URI to contain %q, got %s", expected, uri)
				}
			}
		})
	}
}

func TestConfig_buildTLSConfig(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name       string
		config     *Config
		expectErr  bool
		assertions func(t *testing.T, tlsCfg *tls.Config)
	}{
		{
			name: "require builds with insecure skip verify",
			config: &Config{
				Host: "localhost",
				SSLConfiguration: &utils.SSLConfig{
					Mode: utils.SSLModeRequire,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if !tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify=true for require")
				}
				if tlsCfg.MinVersion != tls.VersionTLS12 {
					t.Fatalf("expected MinVersion TLS1.2")
				}
			},
		},
		{
			name: "verify-ca builds with callback",
			config: &Config{
				Host: "localhost",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if !tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify=true for verify-ca")
				}
				if tlsCfg.VerifyPeerCertificate == nil {
					t.Fatalf("expected VerifyPeerCertificate callback to be set for verify-ca")
				}
			},
		},
		{
			name: "verify-full sets server name and cert chain validation",
			config: &Config{
				Host: "db.internal",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyFull,
					ServerCA: certs.CACert,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if tlsCfg.InsecureSkipVerify {
					t.Fatalf("expected InsecureSkipVerify=false for verify-full")
				}
				if tlsCfg.ServerName != "db.internal" {
					t.Fatalf("expected ServerName db.internal, got %s", tlsCfg.ServerName)
				}
			},
		},
		{
			name: "verify-full loads client cert and key",
			config: &Config{
				Host: "db.internal",
				SSLConfiguration: &utils.SSLConfig{
					Mode:       utils.SSLModeVerifyFull,
					ServerCA:   certs.CACert,
					ClientCert: certs.ClientCert,
					ClientKey:  certs.ClientKey,
				},
			},
			expectErr: false,
			assertions: func(t *testing.T, tlsCfg *tls.Config) {
				if len(tlsCfg.Certificates) == 0 {
					t.Fatalf("expected client cert to be loaded")
				}
			},
		},
		{
			name: "invalid ca returns error",
			config: &Config{
				Host: "localhost",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: "not-a-pem",
				},
			},
			expectErr: true,
			assertions: func(t *testing.T, _ *tls.Config) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tlsCfg, err := tt.config.buildTLSConfig()
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
			if tlsCfg == nil {
				t.Fatalf("expected tls config, got nil")
			}
			tt.assertions(t, tlsCfg)
		})
	}
}

func TestConfig_ValidateBackfillHost(t *testing.T) {
	newConfig := func(backfillHost string) *Config {
		return &Config{
			Host:          "writer.internal",
			BackfillHost:  backfillHost,
			Port:          5432,
			Username:      "olake",
			Password:      "secret",
			Database:      "ebdb",
			JDBCURLParams: map[string]string{"connect_timeout": "10"},
			SSLConfiguration: &utils.SSLConfig{
				Mode: utils.SSLModeRequire,
			},
		}
	}

	t.Run("reader connection keeps every writer setting except the host", func(t *testing.T) {
		config := newConfig("reader.internal")
		if err := config.Validate(); err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if config.BackfillConnection == nil {
			t.Fatalf("expected a backfill connection URL")
		}
		if config.BackfillConnection.Host != "reader.internal:5432" {
			t.Fatalf("expected reader.internal:5432, got %s", config.BackfillConnection.Host)
		}
		writer, reader := *config.Connection, *config.BackfillConnection
		writer.Host, reader.Host = "", ""
		if writer.String() != reader.String() {
			t.Fatalf("expected identical credentials, database and parameters, got writer %s and reader %s", writer.String(), reader.String())
		}
	})

	t.Run("brackets an IPv6 backfill host", func(t *testing.T) {
		config := newConfig("::1")
		if err := config.Validate(); err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if config.BackfillConnection.Host != "[::1]:5432" {
			t.Fatalf("expected [::1]:5432, got %s", config.BackfillConnection.Host)
		}
	})

	t.Run("no backfill host means writer-only reads", func(t *testing.T) {
		config := newConfig("")
		if err := config.Validate(); err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if config.BackfillConnection != nil {
			t.Fatalf("expected no backfill connection, got %s", config.BackfillConnection.String())
		}
	})

	t.Run("rejects a scheme in the backfill host", func(t *testing.T) {
		if err := newConfig("https://reader.internal").Validate(); err == nil {
			t.Fatalf("expected an error for a backfill host with a scheme")
		}
	})
}

func TestConfig_buildBackfillTLSConfigVerifiesTheReaderHost(t *testing.T) {
	certs := testutils.GenerateTestCerts()
	config := &Config{
		Host:         "writer.internal",
		BackfillHost: "reader.internal",
		SSLConfiguration: &utils.SSLConfig{
			Mode:     utils.SSLModeVerifyFull,
			ServerCA: certs.CACert,
		},
	}
	tlsCfg, err := config.buildBackfillTLSConfig()
	if err != nil {
		t.Fatalf("expected no error but got: %v", err)
	}
	if tlsCfg.ServerName != "reader.internal" {
		t.Fatalf("expected verify-full against reader.internal, got %s", tlsCfg.ServerName)
	}
}
