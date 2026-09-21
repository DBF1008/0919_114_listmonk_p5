// Package subimporter implements a bulk ZIP/CSV importer of subscribers.
// It implements a simple queue for buffering imports and committing records
// to DB along with ZIP and CSV handling utilities. It is meant to be used as
// a singleton as each Importer instance is stateful, where it keeps track of
// an import in progress. Only one import should happen on a single importer
// instance at a time.
package subimporter

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/knadh/listmonk/internal/i18n"
	"github.com/knadh/listmonk/internal/utils"
	"github.com/knadh/listmonk/models"
	"github.com/lib/pq"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	// commitBatchSize is the number of inserts to commit in a single SQL transaction.
	commitBatchSize = 10000

	// maxFailedLines is the maximum number of failed line records kept in
	// memory for the final summary report.
	maxFailedLines = 1000

	// checkpointInterval is how often (in processed lines) the import
	// progress checkpoint is persisted to the DB for crash recovery.
	checkpointInterval = 10000

	// estimateInterval is how often (in processed lines) the total line
	// count is re-estimated from the bytes consumed while streaming.
	estimateInterval = 500
)

// Various import statuses.
const (
	StatusNone      = "none"
	StatusImporting = "importing"
	StatusStopping  = "stopping"
	StatusFinished  = "finished"
	StatusFailed    = "failed"

	ModeSubscribe = "subscribe"
	ModeBlocklist = "blocklist"
)

// Importer represents the bulk CSV subscriber import system.
type Importer struct {
	opt  Options
	db   *sql.DB
	i18n *i18n.I18n

	domainBlocklist       map[string]struct{}
	hasBlocklistWildcards bool
	hasBlocklist          bool

	domainAllowlist       map[string]struct{}
	hasAllowlistWildcards bool
	hasAllowlist          bool

	stop   chan bool
	status Status
	sync.RWMutex
}

// Options represents import options.
type Options struct {
	UpsertStmt         *sql.Stmt
	BlocklistStmt      *sql.Stmt
	UpdateListDateStmt *sql.Stmt
	PostCB             func(subject string, data any) error

	DomainBlocklist []string
	DomainAllowlist []string
}

// Session represents a single import session.
type Session struct {
	im       *Importer
	subQueue chan SubReq
	log      *log.Logger

	opt SessionOpt
}

// SessionOpt represents the options for an importer session.
type SessionOpt struct {
	Filename           string `json:"filename"`
	Mode               string `json:"mode"`
	SubStatus          string `json:"subscription_status"`
	Overwrite          bool   `json:"overwrite"`
	OverwriteUserInfo  bool   `json:"overwrite_userinfo"`
	OverwriteSubStatus bool   `json:"overwrite_subscription_status"`
	Delim              string `json:"delim"`
	ListIDs            []int  `json:"lists"`
}

// Status represents statistics from an ongoing import session.
type Status struct {
	Name        string       `json:"name"`
	Total       int          `json:"total"`
	Processed   int          `json:"processed"`
	Imported    int          `json:"imported"`
	Failed      int          `json:"failed"`
	Status      string       `json:"status"`
	StartedAt   time.Time    `json:"started_at"`
	Percent     float64      `json:"percent"`
	Rate        float64      `json:"rate"`
	ETA         int          `json:"eta"`
	FailedLines []FailedLine `json:"failed_lines,omitempty"`
	logBuf      *bytes.Buffer
}

// FailedLine records a single CSV line that could not be imported,
// along with the reason for the failure.
type FailedLine struct {
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

// importCheckpoint is the resumable state of an import session that's
// persisted to the DB so that an import can resume after a restart.
type importCheckpoint struct {
	Filename  string `json:"filename"`
	Processed int    `json:"processed"`
	Imported  int    `json:"imported"`
	Failed    int    `json:"failed"`
}

// SubReq is a wrapper over the Subscriber model.
type SubReq struct {
	models.Subscriber
	Lists          []int    `json:"lists"`
	ListUUIDs      []string `json:"list_uuids"`
	PreconfirmSubs bool     `json:"preconfirm_subscriptions"`
}

type importStatusTpl struct {
	Name     string
	Status   string
	Imported int
	Failed   int
	Total    int
}

var (
	// ErrIsImporting is thrown when an import request is made while an
	// import is already running.
	ErrIsImporting = errors.New("import is already running")

	csvHeaders = map[string]bool{
		"email":      true,
		"name":       true,
		"attributes": true}

	regexCleanStr = regexp.MustCompile("[[:^ascii:]]")
)

// New returns a new instance of Importer.
func New(opt Options, db *sql.DB, i *i18n.I18n) *Importer {
	im := Importer{
		opt:             opt,
		db:              db,
		i18n:            i,
		domainBlocklist: make(map[string]struct{}, len(opt.DomainBlocklist)),
		domainAllowlist: make(map[string]struct{}, len(opt.DomainAllowlist)),
		status:          Status{Status: StatusNone, logBuf: bytes.NewBuffer(nil)},
		stop:            make(chan bool, 1),
	}

	// Domain blocklist.
	mp, hasWildcards := makeDomainMap(opt.DomainBlocklist)
	im.domainBlocklist = mp
	im.hasBlocklistWildcards = hasWildcards
	im.hasBlocklist = len(mp) > 0

	// Domain allowlist.
	mp, hasWildcards = makeDomainMap(opt.DomainAllowlist)
	im.domainAllowlist = mp
	im.hasAllowlistWildcards = hasWildcards
	im.hasAllowlist = len(mp) > 0

	// Create the checkpoint table used for crash recovery (best effort).
	// If this fails, imports still work, just without resume support.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS import_checkpoints (
		id         SMALLINT PRIMARY KEY,
		filename   TEXT NOT NULL,
		processed  BIGINT NOT NULL DEFAULT 0,
		imported   BIGINT NOT NULL DEFAULT 0,
		failed     BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	)`); err != nil {
		log.Printf("error creating import_checkpoints table (resume disabled): %v", err)
	}

	return &im
}

// NewSession returns an new instance of Session. It takes the name
// of the uploaded file, but doesn't do anything with it but retains it for stats.
func (im *Importer) NewSession(opt SessionOpt) (*Session, error) {
	if !im.isDone() {
		return nil, errors.New("an import is already running")
	}

	// For API backwards compatibility, if the old 'overwrite'
	// field is set, set both overwrite fields to true.
	if opt.Overwrite {
		opt.OverwriteUserInfo = true
		opt.OverwriteSubStatus = true
	}

	im.Lock()
	im.status = Status{Status: StatusImporting,
		Name:      opt.Filename,
		StartedAt: time.Now(),
		logBuf:    bytes.NewBuffer(nil)}
	im.Unlock()

	s := &Session{
		im:       im,
		log:      log.New(im.status.logBuf, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile),
		subQueue: make(chan SubReq, commitBatchSize),
		opt:      opt,
	}

	s.log.Printf("processing '%s'", opt.Filename)
	return s, nil
}

// GetStats returns the global Stats of the importer.
func (im *Importer) GetStats() Status {
	im.RLock()
	defer im.RUnlock()

	out := Status{
		Name:        im.status.Name,
		Status:      im.status.Status,
		Total:       im.status.Total,
		Processed:   im.status.Processed,
		Imported:    im.status.Imported,
		Failed:      im.status.Failed,
		StartedAt:   im.status.StartedAt,
		FailedLines: im.status.FailedLines,
	}

	// Derive the import speed (lines/sec) from the elapsed time.
	if !out.StartedAt.IsZero() && out.Processed > 0 {
		if elapsed := time.Since(out.StartedAt).Seconds(); elapsed > 0 {
			out.Rate = float64(out.Processed) / elapsed
		}
	}

	// Derive the completion percentage and ETA from the rate.
	if out.Total > 0 {
		out.Percent = float64(out.Processed) / float64(out.Total) * 100
		if out.Percent > 100 {
			out.Percent = 100
		}
		if out.Rate > 0 && out.Processed < out.Total {
			out.ETA = int(float64(out.Total-out.Processed) / out.Rate)
		}
	}

	return out
}

// GetLogs returns the log entries of the last import session.
func (im *Importer) GetLogs() []byte {
	im.RLock()
	defer im.RUnlock()

	if im.status.logBuf == nil {
		return []byte{}
	}

	return im.status.logBuf.Bytes()
}

// setStatus sets the Importer's status.
func (im *Importer) setStatus(status string) {
	im.Lock()
	im.status.Status = status
	im.Unlock()
}

// getStatus get's the Importer's status.
func (im *Importer) getStatus() string {
	im.RLock()
	status := im.status.Status
	im.RUnlock()
	return status
}

// isDone returns true if the importer is working (importing|stopping).
func (im *Importer) isDone() bool {
	s := true
	im.RLock()
	if im.getStatus() == StatusImporting || im.getStatus() == StatusStopping {
		s = false
	}
	im.RUnlock()

	return s
}

// incrementImportCount sets the Importer's "imported" counter.
func (im *Importer) incrementImportCount(n int) {
	im.Lock()
	im.status.Imported += n
	im.Unlock()
}

// incrementProcessedCount increments the Importer's "processed" counter.
func (im *Importer) incrementProcessedCount(n int) {
	im.Lock()
	im.status.Processed += n
	im.Unlock()
}

// incrementFailedCount increments the Importer's "failed" counter.
func (im *Importer) incrementFailedCount(n int) {
	im.Lock()
	im.status.Failed += n
	im.Unlock()
}

// recordFailedLine records a failed CSV line (number + reason) and
// increments the "failed" counter. The list of failed lines kept in
// memory is capped at maxFailedLines.
func (im *Importer) recordFailedLine(line int, reason string) {
	im.Lock()
	im.status.Failed++
	if len(im.status.FailedLines) < maxFailedLines {
		im.status.FailedLines = append(im.status.FailedLines, FailedLine{Line: line, Reason: reason})
	}
	im.Unlock()
}

// setTotal sets the Importer's "total" counter.
func (im *Importer) setTotal(n int) {
	im.Lock()
	im.status.Total = n
	im.Unlock()
}

// loadCheckpoint loads the persisted import checkpoint from the DB, if any.
func (im *Importer) loadCheckpoint() (importCheckpoint, bool) {
	var cp importCheckpoint
	err := im.db.QueryRow(`SELECT filename, processed, imported, failed FROM import_checkpoints WHERE id = 1`).
		Scan(&cp.Filename, &cp.Processed, &cp.Imported, &cp.Failed)
	if err != nil {
		return cp, false
	}
	return cp, true
}

// saveCheckpoint persists the import checkpoint to the DB.
func (im *Importer) saveCheckpoint(cp importCheckpoint) error {
	_, err := im.db.Exec(`INSERT INTO import_checkpoints (id, filename, processed, imported, failed, updated_at)
		VALUES (1, $1, $2, $3, $4, NOW())
		ON CONFLICT (id) DO UPDATE SET filename = $1, processed = $2, imported = $3, failed = $4, updated_at = NOW()`,
		cp.Filename, cp.Processed, cp.Imported, cp.Failed)
	return err
}

// clearCheckpoint deletes the persisted import checkpoint from the DB.
func (im *Importer) clearCheckpoint() {
	if _, err := im.db.Exec(`DELETE FROM import_checkpoints WHERE id = 1`); err != nil {
		log.Printf("error clearing import checkpoint: %v", err)
	}
}

// sendNotif sends admin notifications for import completions.
func (im *Importer) sendNotif(status string) error {
	var (
		s   = im.GetStats()
		out = importStatusTpl{
			Name:     s.Name,
			Status:   status,
			Imported: s.Imported,
			Failed:   s.Failed,
			Total:    s.Total,
		}
		subject = fmt.Sprintf("%s: %s import", cases.Title(language.Und).String(status), s.Name)
	)
	return im.opt.PostCB(subject, out)
}

// Start is a blocking function that selects on a channel queue until all
// subscriber entries in the import session are imported. It should be
// invoked as a goroutine.
func (s *Session) Start() {
	var (
		tx    *sql.Tx
		stmt  *sql.Stmt
		err   error
		total = 0
		cur   = 0
	)

	listIDs := make([]int, len(s.opt.ListIDs))
	copy(listIDs, s.opt.ListIDs)

	for sub := range s.subQueue {
		if cur == 0 {
			// New transaction batch.
			tx, err = s.im.db.Begin()
			if err != nil {
				s.log.Printf("error creating DB transaction: %v", err)
				continue
			}

			if s.opt.Mode == ModeSubscribe {
				stmt = tx.Stmt(s.im.opt.UpsertStmt)
			} else {
				stmt = tx.Stmt(s.im.opt.BlocklistStmt)
			}
		}

		uu, err := uuid.NewV4()
		if err != nil {
			s.log.Printf("error generating UUID: %v", err)
			tx.Rollback()
			s.im.incrementFailedCount(cur + 1)
			cur = 0
			continue
		}

		if s.opt.Mode == ModeSubscribe {
			_, err = stmt.Exec(uu, sub.Email, sub.Name, sub.Attribs, pq.Array(listIDs), s.opt.SubStatus, s.opt.OverwriteUserInfo, s.opt.OverwriteSubStatus)
		} else if s.opt.Mode == ModeBlocklist {
			_, err = stmt.Exec(uu, sub.Email, sub.Name, sub.Attribs)
		}
		if err != nil {
			// Don't abort the whole import on a bad batch. Roll back the
			// current transaction, count the records in it as failed and
			// continue with a fresh transaction (partial failure mode).
			s.log.Printf("error executing insert: %v", err)
			tx.Rollback()
			s.im.incrementFailedCount(cur + 1)
			cur = 0
			continue
		}
		cur++
		total++

		// Batch size is met. Commit.
		if cur%commitBatchSize == 0 {
			if err := tx.Commit(); err != nil {
				tx.Rollback()
				s.log.Printf("error committing to DB: %v", err)
				s.im.incrementFailedCount(cur)
			} else {
				s.im.incrementImportCount(cur)
				s.log.Printf("imported %d", total)
			}

			cur = 0
		}
	}

	// Queue's closed and there's nothing left to commit.
	if cur == 0 {
		s.finish()
		if _, err := s.im.opt.UpdateListDateStmt.Exec(pq.Array(listIDs)); err != nil {
			s.log.Printf("error updating lists date: %v", err)
		}
		s.im.sendNotif(StatusFinished)
		return
	}

	// Queue's closed and there are records left to commit.
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		s.im.setStatus(StatusFailed)
		s.log.Printf("error committing to DB: %v", err)
		s.im.incrementFailedCount(cur)
		s.im.sendNotif(StatusFailed)
		return
	}

	s.im.incrementImportCount(cur)
	s.finish()
	if _, err := s.im.opt.UpdateListDateStmt.Exec(pq.Array(listIDs)); err != nil {
		s.log.Printf("error updating lists date: %v", err)
	}

	s.im.sendNotif(StatusFinished)
}

// finish wraps up an import session: it clears the resume checkpoint (unless
// the import was stopped midway, in which case the checkpoint is kept so the
// import can be resumed) and logs the final summary report.
func (s *Session) finish() {
	if s.im.getStatus() == StatusStopping {
		s.log.Printf("import stopped midway; checkpoint saved. Re-upload the same file to resume")
	} else {
		s.im.clearCheckpoint()
	}

	s.im.setStatus(StatusFinished)

	stats := s.im.GetStats()
	s.log.Printf("import finished: %d imported, %d failed, %d processed, %d total in %.0fs",
		stats.Imported, stats.Failed, stats.Processed, stats.Total, time.Since(stats.StartedAt).Seconds())

	// Print the summary of failed lines (line number + reason).
	if len(stats.FailedLines) > 0 {
		s.log.Printf("failed lines report (%d recorded, %d total failures):",
			len(stats.FailedLines), stats.Failed)
		for _, fl := range stats.FailedLines {
			s.log.Printf("  line %d: %s", fl.Line, fl.Reason)
		}
	}
}

// Stop stops an active import session.
func (s *Session) Stop() {
	close(s.subQueue)
}

// LoadZIP opens the ZIP file at srcPath and streams the first CSV file found
// in it into the importer without extracting it to disk. Only one CSV from
// the ZIP is considered. If multiple files have to be processed, counting the
// net number of lines (to track progress), keeping the global import state
// (failed / successful) etc. across multiple files becomes complex. Instead,
// it's just easier for the end user to concat multiple CSVs (if there are
// multiple in the first place) and upload as one. The ZIP file at srcPath is
// treated as a temporary upload and is deleted once the import is done.
func (s *Session) LoadZIP(srcPath string, delim rune) error {
	// Clean up the temporary uploaded ZIP file.
	defer func() {
		if err := os.Remove(srcPath); err != nil {
			s.log.Printf("error removing temporary file '%s': %v", srcPath, err)
		}
	}()

	if s.im.isDone() {
		return ErrIsImporting
	}

	failed := true
	defer func() {
		if failed {
			s.im.setStatus(StatusFailed)
		}
	}()

	z, err := zip.OpenReader(srcPath)
	if err != nil {
		return err
	}
	defer z.Close()

	for _, f := range z.File {
		fName := f.FileInfo().Name()

		// Skip directories.
		if f.FileInfo().IsDir() {
			s.log.Printf("skipping directory '%s'", fName)
			continue
		}

		// Skip files without the .csv extension.
		if !strings.HasSuffix(strings.ToLower(fName), ".csv") {
			s.log.Printf("skipping non .csv file '%s'", fName)
			continue
		}

		// Sanitize the file name to prevent ZIP slip path traversal.
		fName = filepath.Base(fName)

		s.log.Printf("streaming '%s' from ZIP", fName)
		src, err := f.Open()
		if err != nil {
			s.log.Printf("error opening '%s' from ZIP: '%v'", fName, err)
			return err
		}
		defer src.Close()

		err = s.LoadCSV(src, int64(f.UncompressedSize64), delim)
		failed = false
		return err
	}

	s.log.Println("no CSV files found in the ZIP")
	return errors.New("no CSV files found in the ZIP")
}

// LoadCSV streams a CSV file from the given reader and validates and imports
// the subscriber entries in it. Malformed lines are skipped and recorded
// (line number + reason) instead of aborting the whole import, and a summary
// report is logged at the end. sizeBytes is the total size of the source in
// bytes (0 if unknown) and is used to estimate progress while streaming.
// Progress checkpoints are persisted to the DB periodically so that an
// interrupted import (eg: service restart) can be resumed by re-uploading
// the same file.
func (s *Session) LoadCSV(src io.Reader, sizeBytes int64, delim rune) error {
	if s.im.isDone() {
		return ErrIsImporting
	}

	// Default status is "failed" in case the function
	// returns at one of the many possible errors.
	failed := true
	defer func() {
		if failed {
			s.im.setStatus(StatusFailed)
		}
	}()

	// Wrap the source in a counting reader to track the bytes consumed,
	// used to estimate the total number of lines while streaming.
	cr := &countingReader{r: src}
	rd := csv.NewReader(cr)
	rd.Comma = delim
	rd.LazyQuotes = true

	// Read the header.
	csvHdr, err := rd.Read()
	if err != nil {
		if err == io.EOF {
			return errors.New("empty file")
		}
		s.log.Printf("error reading header: '%v'", err)
		return err
	}

	hdrKeys := s.mapCSVHeaders(csvHdr, csvHeaders)
	// email is a required header.
	if _, ok := hdrKeys["email"]; !ok {
		s.log.Printf("'email' column not found in the CSV")
		return errors.New("'email' column not found")
	}

	// If there's a checkpoint from a previous interrupted import of the
	// same file, resume from it by skipping the already processed lines.
	resumeFrom := 0
	if cp, ok := s.im.loadCheckpoint(); ok && cp.Filename == s.opt.Filename && cp.Processed > 0 {
		resumeFrom = cp.Processed

		s.im.Lock()
		s.im.status.Imported = cp.Imported
		s.im.status.Failed = cp.Failed
		s.im.status.Processed = cp.Processed
		s.im.Unlock()

		s.log.Printf("resuming import of '%s': skipping %d processed lines (%d imported, %d failed so far)",
			s.opt.Filename, cp.Processed, cp.Imported, cp.Failed)
	}

	var (
		lnHdr = len(hdrKeys)
		i     = 0
	)
	for {
		// Check for the stop signal.
		select {
		case <-s.im.stop:
			failed = false
			s.saveCheckpointNow(i)
			close(s.subQueue)
			s.log.Println("stop request received")
			return nil
		default:
		}

		cols, err := rd.Read()
		if err == io.EOF {
			break
		}
		i++

		// Skip lines that were already processed before the interruption.
		if i <= resumeFrom {
			continue
		}

		s.im.incrementProcessedCount(1)

		// Periodically estimate the total number of lines from the bytes
		// consumed so far and persist a resume checkpoint.
		if i%estimateInterval == 0 && sizeBytes > 0 && cr.n > 0 {
			s.im.setTotal(int(float64(i) * float64(sizeBytes) / float64(cr.n)))
		}
		if i%checkpointInterval == 0 {
			s.saveCheckpointNow(i)
		}

		// Skip malformed lines (partial failure mode) instead of
		// aborting the whole import.
		if err != nil {
			reason := fmt.Sprintf("CSV parse error: %v", err)
			s.log.Printf("skipping line %d. %s", i, reason)
			s.im.recordFailedLine(i, reason)
			continue
		}

		lnCols := len(cols)
		if lnCols < lnHdr {
			reason := fmt.Sprintf("column count (%d) does not match minimum header count (%d)", lnCols, lnHdr)
			s.log.Printf("skipping line %d. %s", i, reason)
			s.im.recordFailedLine(i, reason)
			continue
		}

		// Iterate the key map and based on the indices mapped earlier,
		// form a map of key: csv_value, eg: email: user@user.com.
		row := make(map[string]string, lnCols)
		for key := range hdrKeys {
			row[key] = cols[hdrKeys[key]]
		}

		sub := SubReq{}
		sub.Email = row["email"]

		if v, ok := row["name"]; ok {
			sub.Name = v
		}

		sub, err = s.im.ValidateFields(sub)
		if err != nil {
			reason := fmt.Sprintf("%v", err)
			s.log.Printf("skipping line %d: %v: %v", i, err, cols)
			s.im.recordFailedLine(i, reason)
			continue
		}

		// JSON attributes.
		if len(row["attributes"]) > 0 {
			var (
				attribs models.JSON
				b       = []byte(row["attributes"])
			)
			if err := json.Unmarshal(b, &attribs); err != nil {
				s.log.Printf("skipping invalid attributes JSON on line %d for '%s': %v", i, sub.Email, err)
			} else {
				sub.Attribs = attribs
			}
		}

		// Send the subscriber to the queue.
		s.subQueue <- sub
	}

	// Now that the whole file's been read, the line count is exact.
	s.im.setTotal(i)
	s.saveCheckpointNow(i)

	close(s.subQueue)
	failed = false

	return nil
}

// saveCheckpointNow persists the current import progress to the DB. The
// number of processed lines stored is the number of records resolved so far
// (imported + failed), which lags the read position slightly. This is
// deliberate: on resume, a handful of boundary lines may be re-processed,
// which is safe as the inserts are idempotent upserts.
func (s *Session) saveCheckpointNow(readLines int) {
	stats := s.im.GetStats()
	resolved := stats.Imported + stats.Failed
	if resolved > readLines {
		resolved = readLines
	}

	if err := s.im.saveCheckpoint(importCheckpoint{
		Filename:  s.opt.Filename,
		Processed: resolved,
		Imported:  stats.Imported,
		Failed:    stats.Failed,
	}); err != nil {
		s.log.Printf("error saving import checkpoint: %v", err)
	}
}

// Stop sends a signal to stop the existing import.
func (im *Importer) Stop() {
	if im.getStatus() != StatusImporting {
		im.Lock()
		im.status = Status{Status: StatusNone}
		im.Unlock()

		return
	}

	select {
	case im.stop <- true:
		im.setStatus(StatusStopping)
	default:
	}
}

// SanitizeEmail validates and sanitizes an e-mail string and returns the
// canonical (lowercased, trimmed) address. Domain allowlist/blocklist rules
// are enforced on top of the bare-address validation in utils.SanitizeEmail.
func (im *Importer) SanitizeEmail(email string) (string, error) {
	addr, err := utils.SanitizeEmail(email)
	if err != nil {
		return "", errors.New(im.i18n.T("subscribers.invalidEmail"))
	}

	// Check if the e-mail's domain is blocklisted. The e-mail domain and blocklist config
	// are always lowercase.
	if im.hasAllowlist || im.hasBlocklist {
		d := strings.Split(addr, "@")
		if len(d) != 2 {
			return addr, nil
		}

		domain := d[1]

		// If there's an allowlist, check if the domain is in it. Checking blocklist after that is moot.
		if im.hasAllowlist {
			if !im.checkInList(domain, im.hasAllowlistWildcards, im.domainAllowlist) {
				return "", errors.New(im.i18n.T("subscribers.domainBlocklisted"))
			}
		} else if im.hasBlocklist {
			if im.checkInList(domain, im.hasBlocklistWildcards, im.domainBlocklist) {
				return "", errors.New(im.i18n.T("subscribers.domainBlocklisted"))
			}
		}
	}

	return addr, nil
}

// ValidateFields validates incoming subscriber field values and returns sanitized fields.
func (im *Importer) ValidateFields(s SubReq) (SubReq, error) {
	if len(s.Email) > 1000 {
		return s, errors.New(im.i18n.T("subscribers.invalidEmail"))
	}

	em, err := im.SanitizeEmail(s.Email)
	if err != nil {
		return s, err
	}
	s.Email = strings.ToLower(em)

	// If there's no name, use the name part of the e-mail.
	s.Name = strings.TrimSpace(s.Name)
	if len(s.Name) == 0 {
		name := strings.ToLower(strings.Split(s.Email, "@")[0])

		parts := strings.Fields(strings.ReplaceAll(name, ".", " "))
		for n, p := range parts {
			parts[n] = cases.Title(language.Und).String(p)
		}

		s.Name = strings.Join(parts, " ")
	}

	return s, nil
}

// Check the domain against the given map of domains (block/allowlist).
func (im *Importer) checkInList(domain string, hasWildcards bool, mp map[string]struct{}) bool {
	// Check the domain as-is.
	if _, ok := mp[domain]; ok {
		return true
	}

	// If there are wildcards in the list and the email domain has a subdomain, check that.
	if hasWildcards && strings.Count(domain, ".") > 1 {
		parts := strings.Split(domain, ".")

		// Replace the first part of the subdomain with * and check if that exists in the list.
		// Eg: test.mail.example.com => *.mail.example.com
		parts[0] = "*"
		domain = strings.Join(parts, ".")

		if _, ok := mp[domain]; ok {
			return true
		}
	}

	return false
}

// mapCSVHeaders takes a list of headers obtained from a CSV file, a map of known headers,
// and returns a new map with each of the headers in the known map mapped by the position (0-n)
// in the given CSV list.
func (s *Session) mapCSVHeaders(csvHdrs []string, knownHdrs map[string]bool) map[string]int {
	// Map 0-n column index to the header keys, name: 0, email: 1 etc.
	// This is to allow dynamic ordering of columns in th CSV.
	hdrKeys := make(map[string]int)
	for i, h := range csvHdrs {
		// Clean the string of non-ASCII characters (BOM etc.).
		h := regexCleanStr.ReplaceAllString(strings.TrimSpace(h), "")
		if _, ok := knownHdrs[h]; !ok {
			s.log.Printf("ignoring unknown header '%s'", h)
			continue
		}
		hdrKeys[h] = i
	}

	return hdrKeys
}

// countingReader wraps an io.Reader and counts the number of bytes read.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func makeDomainMap(domains []string) (map[string]struct{}, bool) {
	var (
		out          = make(map[string]struct{}, len(domains))
		hasWildCards = false
	)
	for _, d := range domains {
		out[d] = struct{}{}

		// Domains with *. as the subdomain prefix, strip that
		// and add the full domain to the blocklist as well.
		// eg: *.example.com => example.com
		if strings.Contains(d, "*.") {
			hasWildCards = true
			out[strings.TrimPrefix(d, "*.")] = struct{}{}
		}
	}

	return out, hasWildCards
}
