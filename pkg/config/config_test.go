package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	mysqlsource "github.com/duy-tung/cdc-scaling/pkg/source/mysql"
)

// TestStateCredentialsDefaultPort is a regression test: with the source
// port omitted, the derived state-store address must get the MySQL default
// port, not the zero value ("host:0").
func TestStateCredentialsDefaultPort(t *testing.T) {
	h := &Hyperloop{
		ID: "x",
		Source: Source{MySQL: &mysqlsource.Config{
			Host: "db-host", User: "nomios-user",
		}},
		State: State{Store: "mysql", Database: "nomios"},
	}
	addr, user, _ := h.stateCredentials()
	if addr != "db-host:3306" {
		t.Fatalf("state addr = %q, want db-host:3306", addr)
	}
	if user != "nomios-user" {
		t.Fatalf("state user = %q", user)
	}

	h.Source.MySQL.Port = 3307
	if addr, _, _ = h.stateCredentials(); addr != "db-host:3307" {
		t.Fatalf("state addr = %q, want db-host:3307", addr)
	}

	h.State.Addr = "elsewhere:3308"
	h.State.User = "other"
	if addr, user, _ = h.stateCredentials(); addr != "elsewhere:3308" || user != "other" {
		t.Fatalf("explicit state addr not honored: %q %q", addr, user)
	}
}

func TestLoadExpandsEnvAndDefaults(t *testing.T) {
	t.Setenv("TEST_NOMIOS_PW", "s3cret")
	path := filepath.Join(t.TempDir(), "nomios.yaml")
	yaml := `
hyperloops:
  - id: hl1
    source:
      mysql:
        host: db
        user: nomios
        password: ${TEST_NOMIOS_PW}
        serverID: 5501
    sink:
      kafka:
        brokers: ["k:9092"]
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Listen != ":8080" {
		t.Fatalf("default listen = %q", f.Listen)
	}
	if len(f.Hyperloops) != 1 {
		t.Fatalf("hyperloops = %d", len(f.Hyperloops))
	}
	h := f.Hyperloops[0]
	if h.Source.MySQL.Password != "s3cret" {
		t.Fatalf("env not expanded: %q", h.Source.MySQL.Password)
	}
	if !h.AutoStartEnabled() {
		t.Fatal("autoStart should default to true")
	}
}

// TestLoadRejectsMissingEnv: an undefined ${VAR} must fail loudly, not
// silently become an empty string.
func TestLoadRejectsMissingEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nomios.yaml")
	yaml := "hyperloops:\n  - id: hl1\n    source:\n      mysql:\n        password: ${DEFINITELY_UNSET_NOMIOS_VAR}\n"
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for undefined environment variable")
	}
	if want := "DEFINITELY_UNSET_NOMIOS_VAR"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name the missing variable %q", err, want)
	}
}
