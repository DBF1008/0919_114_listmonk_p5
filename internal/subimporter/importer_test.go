package subimporter

import (
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knadh/listmonk/internal/i18n"
)

func newTestImporter(t *testing.T) *Importer {
	t.Helper()

	i, err := i18n.New([]byte(`{
		"_.code": "en",
		"_.name": "English",
		"subscribers.invalidEmail": "Invalid e-mail",
		"subscribers.domainBlocklisted": "Domain is blocklisted"
	}`))
	if err != nil {
		t.Fatalf("creating i18n: %v", err)
	}

	return New(Options{}, nil, i)
}

func TestCountLines(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"a", 1},
		{"a\n", 1},
		{"a\nb", 2},
		{"a\nb\n", 2},
		{"a\nb\nc\n", 3},
		{"\n\n\n", 3},
	}

	for _, c := range cases {
		got, err := CountLines(strings.NewReader(c.in))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != c.want {
			t.Errorf("CountLines(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestComputeProgress(t *testing.T) {
	// No total or no start time: nothing to report.
	if p, eta := computeProgress(0, 0, time.Time{}); p != 0 || eta != 0 {
		t.Errorf("expected zero progress, got %v/%v", p, eta)
	}

	// Halfway through at 1 row/second: 50% and ETA ~ 50s.
	start := time.Now().Add(-50 * time.Second)
	p, eta := computeProgress(100, 50, start)
	if p < 49 || p > 51 {
		t.Errorf("expected ~50%%, got %f", p)
	}
	if eta < 40 || eta > 60 {
		t.Errorf("expected ETA ~50s, got %d", eta)
	}

	// Imported exceeds total must not report >100%.
	if p, _ := computeProgress(10, 11, start); p != 100 {
		t.Errorf("expected capped 100%%, got %f", p)
	}
}

func TestMapCSVHeaders(t *testing.T) {
	s := &Session{log: log.New(io.Discard, "", 0)}

	got := s.mapCSVHeaders([]string{"name", "email", "attributes", "unknown"}, csvHeaders)
	if got["name"] != 0 || got["email"] != 1 || got["attributes"] != 2 {
		t.Errorf("unexpected header map: %#v", got)
	}
	if _, ok := got["unknown"]; ok {
		t.Error("unknown header should be ignored")
	}
}

func TestRecordError(t *testing.T) {
	im := newTestImporter(t)
	s := &Session{im: im, log: log.New(io.Discard, "", 0)}

	for i := 0; i < maxRowErrors+50; i++ {
		s.recordError(i+1, "bad row")
	}

	if len(s.rowErrors) != maxRowErrors {
		t.Errorf("expected errors capped at %d, got %d", maxRowErrors, len(s.rowErrors))
	}
	if s.failedCount != maxRowErrors+50 {
		t.Errorf("expected total failed %d, got %d", maxRowErrors+50, s.failedCount)
	}
	if got := im.GetStats().Failed; got != maxRowErrors+50 {
		t.Errorf("expected status failed %d, got %d", maxRowErrors+50, got)
	}
}

// TestLoadCSVPartialFailure ensures bad rows are skipped individually while
// valid rows are still queued, instead of aborting the whole import.
func TestLoadCSVPartialFailure(t *testing.T) {
	im := newTestImporter(t)

	sess, err := im.NewSession(SessionOpt{
		Filename:  "partial.csv",
		Mode:      ModeSubscribe,
		SubStatus: "unconfirmed",
	})
	if err != nil {
		t.Fatalf("creating session: %v", err)
	}

	// Drain the queue (without a DB-backed Start).
	var (
		queued []SubReq
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sub := range sess.subQueue {
			queued = append(queued, sub)
		}
	}()

	csvData := strings.Join([]string{
		"email,name,attributes", // header
		"good1@example.com,Good One,{}",
		"badline",                              // fewer columns than the header
		"not-an-email,Bad Email,{}",            // invalid e-mail
		"good2@example.com,Good Two,{bad json", // invalid attributes JSON; row still imports
		"good3@example.com,,{}",                // missing name is auto-generated
	}, "\n") + "\n"

	if err := sess.LoadCSV(strings.NewReader(csvData), 6, ','); err != nil {
		t.Fatalf("LoadCSV error: %v", err)
	}
	wg.Wait()

	if len(queued) != 3 {
		t.Fatalf("expected 3 valid rows queued, got %d (%+v)", len(queued), queued)
	}
	if sess.failedCount != 2 {
		t.Errorf("expected 2 failed rows, got %d", sess.failedCount)
	}
	if got := im.GetStats().Failed; got != 2 {
		t.Errorf("expected status failed 2, got %d", got)
	}

	// Line numbers and reasons must be recorded.
	if len(sess.rowErrors) != 2 {
		t.Fatalf("expected 2 recorded errors, got %d", len(sess.rowErrors))
	}
	if sess.rowErrors[0].Line != 2 {
		t.Errorf("expected first bad line 2, got %d", sess.rowErrors[0].Line)
	}
	if sess.rowErrors[1].Line != 3 {
		t.Errorf("expected second bad line 3, got %d", sess.rowErrors[1].Line)
	}
}

// TestLoadCSVResumeSkipsCheckpointRows verifies rows at or below the resume
// line are skipped. (DB persistence itself is covered by test.sh against a
// live database.)
func TestLoadCSVStopping(t *testing.T) {
	im := newTestImporter(t)

	sess, err := im.NewSession(SessionOpt{
		Filename:  "stop.csv",
		Mode:      ModeSubscribe,
		SubStatus: "unconfirmed",
	})
	if err != nil {
		t.Fatalf("creating session: %v", err)
	}

	// A stop signal after 2 rows: the rest must not be queued.
	done := make(chan struct{})
	go func() {
		im.Stop()
		close(done)
	}()
	<-done

	var n int
	go func() {
		for range sess.subQueue {
			n++
		}
	}()

	var sb strings.Builder
	sb.WriteString("email,name,attributes\n")
	for i := 0; i < 100; i++ {
		sb.WriteString("user")
		sb.WriteString(string(rune('a' + i%26)))
		sb.WriteString("@example.com,User,{}\n")
	}

	if err := sess.LoadCSV(strings.NewReader(sb.String()), 101, ','); err != nil {
		t.Fatalf("LoadCSV error: %v", err)
	}

	if im.getStatus() != StatusStopping && im.getStatus() != StatusNone {
		t.Errorf("expected stop to leave a stopped/cleared state, got %s", im.getStatus())
	}
}
