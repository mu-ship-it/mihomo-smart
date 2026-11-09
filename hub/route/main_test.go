package route

import (
	"os"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

// TestMain gives the package a throw-away home directory, so the Smart cache file the tests
// write never touches a real profile.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "route-test-home-")
	if err != nil {
		panic(err)
	}
	C.SetHomeDir(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
