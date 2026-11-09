package adapter_test

// Regression tests for the PR #12 crash class (vernesong/mihomo#12, fixed in a866d768).
//
// main() replaces net.DefaultResolver.Dial with a guard that dumps every goroutine and calls
// os.Exit(2). Proxy.StatusTest builds its own http.Transport; when f35e8767 dropped its
// DialContext hook, any probe that got redirected to a plain-http hop fell back to the stdlib
// dialer, resolved the host through net.DefaultResolver, hit the guard and killed the
// process (≈52 times a day on one gateway). These tests install an equivalent guard that
// records instead of exiting, and assert that every hop of a status probe is dialed through
// the proxy and the guard is never reached.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/component/ca"
	C "github.com/metacubex/mihomo/constant"
)

// installResolverGuard mirrors the guard in main.go, but records the address instead of
// exiting. The returned func restores the previous resolver settings.
func installResolverGuard(t *testing.T) *atomic.Int64 {
	t.Helper()
	var hits atomic.Int64
	prevDial, prevPreferGo := net.DefaultResolver.Dial, net.DefaultResolver.PreferGo
	net.DefaultResolver.PreferGo = true
	net.DefaultResolver.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		hits.Add(1)
		t.Errorf("net.DefaultResolver.Dial reached (%s %s): in production this is os.Exit(2)", network, address)
		return nil, errors.New("resolver guard: should never be called")
	}
	t.Cleanup(func() {
		net.DefaultResolver.Dial, net.DefaultResolver.PreferGo = prevDial, prevPreferGo
	})
	return &hits
}

// hostMapProxy is a fake outbound: it "proxies" by mapping the requested host name to a
// local listener, and records every address it was asked to dial.
type hostMapProxy struct {
	*outbound.Base
	mu     sync.Mutex
	hosts  map[string]string // "host:port" or "host" -> 127.0.0.1:port
	dialed []string
}

func (p *hostMapProxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	p.mu.Lock()
	hostPort := net.JoinHostPort(metadata.Host, strconv.Itoa(int(metadata.DstPort)))
	p.dialed = append(p.dialed, hostPort)
	addr, ok := p.hosts[hostPort] // exact host:port first, then any port of the host
	if !ok {
		addr, ok = p.hosts[metadata.Host]
	}
	p.mu.Unlock()
	if !ok {
		return nil, errors.New("hostMapProxy: unknown host " + metadata.Host)
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return outbound.NewConn(c, p), nil
}

func (p *hostMapProxy) dialedHosts() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.dialed...)
}

func newHostMapProxy(hosts map[string]string) (*adapter.Proxy, *hostMapProxy) {
	fake := &hostMapProxy{
		Base:  outbound.NewBase(outbound.BaseOption{Name: "fake-node", Type: C.Direct}),
		hosts: hosts,
	}
	return adapter.NewProxy(fake), fake
}

// selfSignedFor returns a TLS certificate for the given DNS names and trusts it in the
// mihomo CA pool for the duration of the test.
func selfSignedFor(t *testing.T, names ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: names[0]},
		DNSNames:              names,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := ca.AddCertificate(string(certPEM)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ca.ResetCertificate)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

func port(t *testing.T, addr string) string {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// The exact production trigger: an https probe answered with a redirect to a plain-http URL.
func TestStatusTestHTTPSRedirectToPlainHTTPStaysOnProxy(t *testing.T) {
	hits := installResolverGuard(t)

	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer plain.Close()

	secure := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://plain.smart-test.invalid:"+port(t, plain.Listener.Addr().String())+"/landing", http.StatusFound)
	}))
	secure.TLS = &tls.Config{Certificates: []tls.Certificate{selfSignedFor(t, "probe.smart-test.invalid")}}
	secure.StartTLS()
	defer secure.Close()

	proxy, fake := newHostMapProxy(map[string]string{
		"probe.smart-test.invalid": secure.Listener.Addr().String(),
		"plain.smart-test.invalid": plain.Listener.Addr().String(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "https://probe.smart-test.invalid:" + port(t, secure.Listener.Addr().String()) + "/?z=1"
	status, ok, err := proxy.StatusTest(ctx, url)

	if hits.Load() != 0 {
		t.Fatalf("status probe resolved a hop outside the proxy %d time(s)", hits.Load())
	}
	if err != nil {
		t.Fatalf("StatusTest error: %v (dialed: %v)", err, fake.dialedHosts())
	}
	if status != http.StatusOK || !ok {
		t.Fatalf("StatusTest = %d ok=%v, want 200 ok=true (dialed: %v)", status, ok, fake.dialedHosts())
	}
	if got := fake.dialedHosts(); len(got) != 2 {
		t.Fatalf("expected both hops to be dialed through the proxy, got %v", got)
	}
}

// Every redirect shape the probe may follow: http->http to another host, a 3-hop chain
// mixing schemes, and a hop to a bare IP. None may touch the stdlib resolver.
func TestStatusTestRedirectChainsNeverUseStdlibResolver(t *testing.T) {
	hits := installResolverGuard(t)

	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer final.Close()
	finalPort := port(t, final.Listener.Addr().String())

	hop2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://c.smart-test.invalid:"+finalPort+"/", http.StatusMovedPermanently)
	}))
	defer hop2.Close()

	hop1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://b.smart-test.invalid:"+port(t, hop2.Listener.Addr().String())+"/", http.StatusTemporaryRedirect)
	}))
	defer hop1.Close()

	proxy, fake := newHostMapProxy(map[string]string{
		"a.smart-test.invalid": hop1.Listener.Addr().String(),
		"b.smart-test.invalid": hop2.Listener.Addr().String(),
		"c.smart-test.invalid": final.Listener.Addr().String(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, _, err := proxy.StatusTest(ctx, "http://a.smart-test.invalid:"+port(t, hop1.Listener.Addr().String())+"/")
	if hits.Load() != 0 {
		t.Fatalf("status probe resolved a hop outside the proxy %d time(s)", hits.Load())
	}
	if err != nil {
		t.Fatalf("StatusTest error: %v", err)
	}
	if status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", status)
	}
	if got := fake.dialedHosts(); len(got) != 3 {
		t.Fatalf("expected 3 proxied hops, got %v", got)
	}
}

// A redirect to a host the proxy cannot reach must surface as a probe error, never as a
// direct (non-proxied) connection attempt.
func TestStatusTestUnreachableRedirectFailsClosed(t *testing.T) {
	hits := installResolverGuard(t)

	start := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://nowhere.smart-test.invalid/", http.StatusFound)
	}))
	defer start.Close()

	proxy, _ := newHostMapProxy(map[string]string{
		"start.smart-test.invalid": start.Listener.Addr().String(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, ok, err := proxy.StatusTest(ctx, "http://start.smart-test.invalid:"+port(t, start.Listener.Addr().String())+"/")
	if hits.Load() != 0 {
		t.Fatalf("status probe resolved a hop outside the proxy %d time(s)", hits.Load())
	}
	if err == nil && ok {
		t.Fatalf("expected the unreachable hop to fail the probe")
	}
}

// Smart's live probing (StatusProbe, used by probeVerdict on traffic and by the host check)
// and the exit probes (TargetExitProbe) share statusRequest with StatusTest. Each is driven
// with default ports so the https -> http downgrade on the same host is followed, which is
// exactly the hop that used to hit the zeroDialer.
func TestAllProbeEntryPointsRedirectToPlainHTTPStayOnProxy(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ip=203.0.113.7\nloc=HK\n"))
	}))
	defer plain.Close()

	secure := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// same host, scheme downgrade; plus an off-host variant for the exit probe
		target := "http://probe.smart-test.invalid" + r.URL.Path
		if r.URL.Path == "/cdn-cgi/trace" {
			target = "http://other.smart-test.invalid/cdn-cgi/trace"
		}
		http.Redirect(w, r, target, http.StatusFound)
	}))
	secure.TLS = &tls.Config{Certificates: []tls.Certificate{selfSignedFor(t, "probe.smart-test.invalid")}}
	secure.StartTLS()
	defer secure.Close()

	hosts := map[string]string{
		"probe.smart-test.invalid:443": secure.Listener.Addr().String(),
		"probe.smart-test.invalid:80":  plain.Listener.Addr().String(),
		"other.smart-test.invalid:80":  plain.Listener.Addr().String(),
	}

	probes := []struct {
		name     string
		run      func(ctx context.Context, p *adapter.Proxy) error
		wantHops int
	}{
		{"StatusTest", func(ctx context.Context, p *adapter.Proxy) error {
			_, _, err := p.StatusTest(ctx, "https://probe.smart-test.invalid/?z=1")
			return err
		}, 2},
		{"StatusProbe", func(ctx context.Context, p *adapter.Proxy) error {
			_, err := p.StatusProbe(ctx, "https://probe.smart-test.invalid/")
			return err
		}, 2},
		{"TargetExitProbe", func(ctx context.Context, p *adapter.Proxy) error {
			_, err := p.TargetExitProbe(ctx, "probe.smart-test.invalid", false)
			return err
		}, 2},
	}
	for _, pr := range probes {
		t.Run(pr.name, func(t *testing.T) {
			hits := installResolverGuard(t)
			proxy, fake := newHostMapProxy(hosts)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := pr.run(ctx, proxy)
			if hits.Load() != 0 {
				t.Fatalf("%s resolved a hop outside the proxy %d time(s)", pr.name, hits.Load())
			}
			got := fake.dialedHosts()
			if len(got) != pr.wantHops {
				t.Fatalf("%s: proxied hops %v (err=%v), want %d", pr.name, got, err, pr.wantHops)
			}
		})
	}
}
