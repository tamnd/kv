package server

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// scanRows issues a scan and decodes the NDJSON lines into rows, decoding the base64 key and
// value back to strings for easy assertions.
func scanRows(t *testing.T, url string) []struct{ Key, Value string } {
	t.Helper()
	code, body := do(t, http.MethodGet, url, nil)
	if code != http.StatusOK {
		t.Fatalf("scan status = %d, body %s", code, body)
	}
	var out []struct{ Key, Value string }
	sc := bufio.NewScanner(strings.NewReader(string(body)))
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		var row jsonScanRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode row %q: %v", line, err)
		}
		out = append(out, struct{ Key, Value string }{decodeB64(t, row.Key), decodeB64orEmpty(t, row.Value)})
	}
	return out
}

func decodeB64orEmpty(t *testing.T, s string) string {
	if s == "" {
		return ""
	}
	return decodeB64(t, s)
}

func seedScan(t *testing.T, hs string) {
	t.Helper()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		do(t, http.MethodPut, hs+"/v1/kv/"+k, strings.NewReader("V"+k))
	}
}

func TestScanFullRange(t *testing.T) {
	hs, _ := newTestServer(t)
	seedScan(t, hs.URL)
	rows := scanRows(t, hs.URL+"/v1/scan")
	if len(rows) != 5 {
		t.Fatalf("rows = %d, want 5", len(rows))
	}
	if rows[0].Key != "a" || rows[0].Value != "Va" {
		t.Fatalf("first row = %+v", rows[0])
	}
	if rows[4].Key != "e" {
		t.Fatalf("last key = %q, want e", rows[4].Key)
	}
}

func TestScanBounds(t *testing.T) {
	hs, _ := newTestServer(t)
	seedScan(t, hs.URL)
	// [b, d): b and c only.
	rows := scanRows(t, hs.URL+"/v1/scan?from=b&to=d")
	if len(rows) != 2 || rows[0].Key != "b" || rows[1].Key != "c" {
		t.Fatalf("bounded scan = %+v", rows)
	}
}

func TestScanReverseAndLimit(t *testing.T) {
	hs, _ := newTestServer(t)
	seedScan(t, hs.URL)
	rows := scanRows(t, hs.URL+"/v1/scan?reverse=true&limit=2")
	if len(rows) != 2 || rows[0].Key != "e" || rows[1].Key != "d" {
		t.Fatalf("reverse+limit scan = %+v", rows)
	}
}

func TestScanPrefix(t *testing.T) {
	hs, _ := newTestServer(t)
	for _, k := range []string{"app:1", "app:2", "z"} {
		do(t, http.MethodPut, hs.URL+"/v1/kv/"+k, strings.NewReader("v"))
	}
	rows := scanRows(t, hs.URL+"/v1/scan?prefix=app:")
	if len(rows) != 2 || rows[0].Key != "app:1" || rows[1].Key != "app:2" {
		t.Fatalf("prefix scan = %+v", rows)
	}
}

func TestScanKeysOnly(t *testing.T) {
	hs, _ := newTestServer(t)
	seedScan(t, hs.URL)
	rows := scanRows(t, hs.URL+"/v1/scan?keys_only=true&limit=1")
	if len(rows) != 1 || rows[0].Key != "a" || rows[0].Value != "" {
		t.Fatalf("keys-only scan = %+v", rows)
	}
}

func TestWatchStreamsChanges(t *testing.T) {
	hs, _ := newTestServer(t)

	// Open the watch in a goroutine and collect events until we have what we expect, then
	// cancel. The writes happen after the stream is established so the feed sees them live.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+"/v1/watch", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("watch status = %d", resp.StatusCode)
	}

	events := make(chan jsonChange, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev jsonChange
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) == nil {
				events <- ev
			}
		}
	}()

	// Give the subscription a moment to register, then write.
	time.Sleep(50 * time.Millisecond)
	do(t, http.MethodPut, hs.URL+"/v1/kv/wk", strings.NewReader("wv"))
	do(t, http.MethodDelete, hs.URL+"/v1/kv/wk", nil)

	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case ev := <-events:
			if decodeB64(t, ev.Key) == "wk" {
				got[ev.Kind] = true
			}
		case <-deadline:
			t.Fatalf("timed out, got kinds %v", got)
		}
	}
	if !got["set"] || !got["delete"] {
		t.Fatalf("missing kinds, got %v", got)
	}
}

func TestWatchSince(t *testing.T) {
	hs, db := newTestServer(t)

	// A write before the watch starts; record its version.
	code, body := do(t, http.MethodPut, hs.URL+"/v1/kv/early", strings.NewReader("1"))
	if code != http.StatusOK {
		t.Fatalf("seed status = %d", code)
	}
	var vr versionResponse
	json.Unmarshal(body, &vr)
	earlyVersion := vr.Version
	_ = db

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+"/v1/watch?since="+itoa(earlyVersion), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer resp.Body.Close()

	events := make(chan jsonChange, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev jsonChange
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) == nil {
				events <- ev
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	do(t, http.MethodPut, hs.URL+"/v1/kv/late", strings.NewReader("2"))

	select {
	case ev := <-events:
		if decodeB64(t, ev.Key) != "late" {
			t.Fatalf("since filter let through %q", decodeB64(t, ev.Key))
		}
		if ev.Version <= earlyVersion {
			t.Fatalf("event version %d <= since %d", ev.Version, earlyVersion)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no late event")
	}
}

func itoa(v uint64) string {
	return strings.TrimSpace(jsonNumber(v))
}

func jsonNumber(v uint64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
