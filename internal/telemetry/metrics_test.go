package telemetry

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// shape is "<type> <sorted labels except component>" of one metric family.
func shapes(t *testing.T, g prometheus.Gatherer, component string) map[string]string {
	t.Helper()
	fams, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]string{}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			var labels []string
			comp := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "component" {
					comp = l.GetValue()
					continue
				}
				labels = append(labels, l.GetName())
			}
			if comp != component {
				t.Errorf("%s: component=%q, want %q", f.GetName(), comp, component)
			}
			sort.Strings(labels)
			out[f.GetName()] = strings.ToLower(f.GetType().String()) + " " + strings.Join(labels, ",")
		}
	}
	return out
}

// P18.3 metric names table: exact names, types and labels.
func TestProtocolMetricGroups(t *testing.T) {
	m := mustMember(t, mustPlatform(t, ""), "erp/sales")
	r := m.Registerer()
	hs, err1 := NewHTTPServerMetrics(r)
	hc, err2 := NewHTTPClientMetrics(r)
	gs, err3 := NewGRPCServerMetrics(r)
	gc, err4 := NewGRPCClientMetrics(r)
	db, err5 := NewDBMetrics(r)
	sm, err6 := NewSecretMetrics(r)
	ev, err7 := NewEventMetrics(r)
	az, err8 := NewAuthzMetrics(r)
	for _, err := range []error{err1, err2, err3, err4, err5, err6, err7, err8} {
		if err != nil {
			t.Fatal(err)
		}
	}
	hs.Requests.WithLabelValues("GET", "/orders/:id", "200").Inc()
	hs.Duration.WithLabelValues("GET", "/orders/:id").Observe(0.01)
	hc.Requests.WithLabelValues("erp/inventory", "POST", "503").Inc()
	hc.Duration.WithLabelValues("erp/inventory", "POST").Observe(0.2)
	gs.Handled.WithLabelValues("erp.sales.v2.Orders", "Get", "OK").Inc()
	gs.Duration.WithLabelValues("erp.sales.v2.Orders", "Get").Observe(0.003)
	gc.Handled.WithLabelValues("erp/inventory", "Reserve", "UNAVAILABLE").Inc()
	gc.Duration.WithLabelValues("erp/inventory", "Reserve").Observe(1)
	gc.Inflight.WithLabelValues("erp/inventory").Inc()
	db.PoolInUse.Set(3)
	db.PoolWait.Observe(0.001)
	db.TxRetries.WithLabelValues("40001").Inc()
	db.IdentityOK.Set(1)
	sm.ReloadFailures.WithLabelValues("PG_PASSWORD").Inc()
	ev.OutboxPending.Set(2)
	ev.OutboxOldestAge.Set(5)
	ev.Published.WithLabelValues("erp.sales.order.confirmed").Inc()
	ev.ConsumerHandled.WithLabelValues("erp.sales.order.confirmed", "applied").Inc()
	ev.ConsumerLag.WithLabelValues("erp.sales.order.confirmed").Set(0.5)
	ev.DLQ.WithLabelValues("erp.sales.order.confirmed").Inc()
	az.BundleAge.Set(12)
	az.ProjectionLag.Set(0)
	az.Denied.WithLabelValues("PERMISSION_MISSING").Inc()

	want := map[string]string{
		"be_http_server_requests_total":   "counter method,route,status_code",
		"be_http_server_duration_seconds": "histogram method,route",
		"be_http_client_requests_total":   "counter method,status_code,target",
		"be_http_client_duration_seconds": "histogram method,target",
		"be_grpc_server_handled_total":    "counter code,method,service",
		"be_grpc_server_duration_seconds": "histogram method,service",
		"be_grpc_client_handled_total":    "counter code,method,target",
		"be_grpc_client_duration_seconds": "histogram method,target",
		"be_outbound_inflight":            "gauge target",
		"be_db_pool_in_use":               "gauge ",
		"be_db_pool_wait_seconds":         "histogram ",
		"be_tx_retries_total":             "counter sqlstate",
		"be_db_identity_ok":               "gauge ",
		"be_secret_reload_failures_total": "counter key",
		"be_outbox_pending":               "gauge ",
		"be_outbox_oldest_age_seconds":    "gauge ",
		"be_events_published_total":       "counter subject",
		"be_consumer_handled_total":       "counter result,subject",
		"be_consumer_lag_seconds":         "gauge subject",
		"be_dlq_messages_total":           "counter subject",
		"be_authz_bundle_age_seconds":     "gauge ",
		"be_authz_projection_lag":         "gauge ",
		"be_authz_denied_total":           "counter reason",
	}
	got := shapes(t, m.Gatherer(), "erp/sales")
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: got %q, want %q", name, got[name], w)
		}
	}
}

// failingRegisterer fails the n-th Register call and records what is registered.
type failingRegisterer struct {
	failAt int
	calls  int
	live   map[prometheus.Collector]bool
}

func (f *failingRegisterer) Register(c prometheus.Collector) error {
	f.calls++
	if f.calls == f.failAt {
		return errors.New("clash")
	}
	f.live[c] = true
	return nil
}

func (f *failingRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := f.Register(c); err != nil {
			panic(err)
		}
	}
}

func (f *failingRegisterer) Unregister(c prometheus.Collector) bool {
	ok := f.live[c]
	delete(f.live, c)
	return ok
}

func TestMetricGroupRegistrationIsAllOrNothing(t *testing.T) {
	f := &failingRegisterer{failAt: 3, live: map[prometheus.Collector]bool{}}
	if _, err := NewGRPCClientMetrics(f); err == nil {
		t.Fatal("want a registration error")
	}
	if len(f.live) != 0 {
		t.Fatalf("%d collectors left registered", len(f.live))
	}
	reg := prometheus.NewRegistry()
	if _, err := NewSecretMetrics(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSecretMetrics(reg); err == nil {
		t.Fatal("second registration accepted")
	}
}

// P18.3 / P19.7: each member's registry is its own, every series carries its component label (OTel
// instruments too), and a shell can aggregate them.
func TestMemberRegistriesAreSeparateAndAggregate(t *testing.T) {
	p := mustPlatform(t, "")
	a, b := mustMember(t, p, "erp/sales"), mustMember(t, p, "erp/inventory")
	for _, m := range []*Member{a, b} {
		hs, err := NewHTTPServerMetrics(m.Registerer())
		if err != nil {
			t.Fatal(err)
		}
		hs.Requests.WithLabelValues("GET", "/x", "200").Inc()
	}
	ctr, err := a.Meter().Int64Counter("erp_sales_orders_created")
	if err != nil {
		t.Fatal(err)
	}
	ctr.Add(context.Background(), 2)

	gotA := shapes(t, a.Gatherer(), "erp/sales")
	if _, ok := gotA["erp_sales_orders_created_total"]; !ok {
		t.Fatalf("OTel instrument missing from the member registry: %v", gotA)
	}
	shapes(t, b.Gatherer(), "erp/inventory")
	if _, ok := shapes(t, b.Gatherer(), "erp/inventory")["erp_sales_orders_created_total"]; ok {
		t.Fatal("member A's instrument leaked into B's registry")
	}

	fams, err := prometheus.Gatherers{a.Gatherer(), b.Gatherer()}.Gather()
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if comps := components(fams, "be_http_server_requests_total"); comps != "erp/inventory,erp/sales" {
		t.Fatalf("aggregated components %q", comps)
	}
	for _, m := range []*Member{a, b} {
		if err := m.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func components(fams []*dto.MetricFamily, name string) string {
	var out []string
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "component" {
					out = append(out, l.GetValue())
				}
			}
		}
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
