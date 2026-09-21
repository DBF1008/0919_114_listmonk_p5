// Package subimporter implements a bulk ZIP/CSV importer of subscribers.
// It implements a simple queue for buffering imports and committing records
// to DB along with ZIP and CSV handling utilities. It is meant to be used as
// a singleton as each Importer instance is stateful, where it keeps track of
// an import in progress. Only one import should happen on a single importer
// instance at a time.
package subimporter

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

	// maxRowErrors is the maximum number of row-level errors kept in memory for
	// the end-of-import summary report. The total failed count is always tracked.
	maxRowErrors = 1000

	// checkpointKey is the `settings` table key under which the import resume
	// checkpoint is persisted so that an interrupted import can resume after a
	// service restart.
	checkpointKey = "import.checkpoint"
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

	// rowErrors holds the (capped) list of skipped rows for the summary report.
	rowErrors []RowError

	// failedCount is the total number of skipped rows (even beyond the cap).
	failedCount int
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
	Name      string    `json:"name"`
	Total     int       `json:"total"`
	Imported  int       `json:"imported"`
	Failed    int       `json:"failed"`
	Percent   float64   `json:"percent"`
	ETA       int64     `json:"eta"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	logBuf    *bytes.Buffer
}

// RowError describes a single CSV row that was skipped during an import.
type RowError struct {
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

// importCheckpoint is persisted to the DB so an interrupted import can resume
// after a service restart by re-uploading the same file.
type importCheckpoint struct {
	Filename  string `json:"filename"`
	Processed int    `json:"processed"`
	Imported  int    `json:"imported"`
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
	Total    int
	Failed   int
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

	s := Status{
		Name:      im.status.Name,
		Status:    im.status.Status,
		Total:     im.status.Total,
		Imported:  im.status.Imported,
		Failed:    im.status.Failed,
		StartedAt: im.status.StartedAt,
	}

	// Derive the progress percentage and the ETA from the elapsed time and the
	// number of rows imported so far.
	s.Percent, s.ETA = computeProgress(s.Total, s.Imported, s.StartedAt)

	return s
}

// computeProgress returns the import progress percentage and the estimated
// number of seconds remaining, based on the average import rate since start.
func computeProgress(total, imported int, startedAt time.Time) (float64, int64) {
	if total <= 0 || startedAt.IsZero() {
		return 0, 0
	}

	percent := float64(imported) / float64(total) * 100
	if percent > 100 {
		percent = 100
	}

	var eta int64
	if imported > 0 {
		if elapsed := time.Since(startedAt).Seconds(); elapsed > 0 {
			rate := float64(imported) / elapsed
			if rate > 0 {
				remaining := float64(total-imported) / rate
				if remaining < 0 {
					remaining = 0
				}
				eta = int64(remaining + 0.5)
			}
		}
	}

	return percent, eta
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

// incrementFailedCount increments the number of rows skipped due to errors.
func (im *Importer) incrementFailedCount(n int) {
	im.Lock()
	im.status.Failed += n
	im.Unlock()
}

// sendNotif sends admin notifications for import completions.
func (im *Importer) sendNotif(status string) error {
	var (
		s   = im.GetStats()
		out = importStatusTpl{
			Name:     s.Name,
			Status:   status,
			Imported: s.Imported,
			Total:    s.Total,
			Failed:   s.Failed,
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
			break
		}

		if s.opt.Mode == ModeSubscribe {
			_, err = stmt.Exec(uu, sub.Email, sub.Name, sub.Attribs, pq.Array(listIDs), s.opt.SubStatus, s.opt.OverwriteUserInfo, s.opt.OverwriteSubStatus)
		} else if s.opt.Mode == ModeBlocklist {
			_, err = stmt.Exec(uu, sub.Email, sub.Name, sub.Attribs)
		}
		if err != nil {
			s.log.Printf("error executing insert: %v", err)
			tx.Rollback()
			break
		}
		cur++
		total++

		// Batch size is met. Commit.
		if cur%commitBatchSize == 0 {
			if err := tx.Commit(); err != nil {
				tx.Rollback()
				s.log.Printf("error committing to DB: %v", err)
			} else {
				s.im.incrementImportCount(cur)
				s.log.Printf("imported %d", total)
			}

			cur = 0
		}
	}

	// Queue's closed and there's nothing left to commit.
	if cur == 0 {
		// A manually stopped import keeps its checkpoint so it can be resumed.
		if s.im.getStatus() != StatusStopping {
			if err := s.im.clearCheckpoint(); err != nil {
				s.log.Printf("error clearing import checkpoint: %v", err)
			}
		}
		s.im.setStatus(StatusFinished)
		s.logSummary()
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
		s.im.sendNotif(StatusFailed)
		return
	}

	if s.im.getStatus() != StatusStopping {
		if err := s.im.clearCheckpoint(); err != nil {
			s.log.Printf("error clearing import checkpoint: %v", err)
		}
	}
	s.im.incrementImportCount(cur)
	s.im.setStatus(StatusFinished)
	s.logSummary()
	if _, err := s.im.opt.UpdateListDateStmt.Exec(pq.Array(listIDs)); err != nil {
		s.log.Printf("error updating lists date: %v", err)
	}

	s.im.sendNotif(StatusFinished)
}

// Stop stops an active import session.
func (s *Session) Stop() {
	close(s.subQueue)
}

// LoadCSV streams a CSV from src and validates and imports the subscriber
// entries in it. numLines is the total number of physical lines in the stream
// (including the header) and is used to derive the progress percentage. The
// caller should obtain it beforehand via CountLines (for seekable uploads this
// means counting first and then rewinding; for ZIP entries, decompressing
// twice). This keeps the import fully streaming and avoids copying the entire
// upload to a temporary file.
//
// Malformed rows do not abort the import. They are skipped individually with
// their line number and reason recorded, and a summary report is written to
// the session log once the stream has been fully consumed. If a checkpoint
// for the same filename exists in the DB (for example, after a service
// restart), rows up to the checkpoint are skipped and the import resumes.
func (s *Session) LoadCSV(src io.Reader, numLines int, delim rune) error {
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

	if numLines == 0 {
		return errors.New("empty file")
	}

	// Exclude the header from count.
	s.im.Lock()
	s.im.status.Total = numLines - 1
	s.im.Unlock()

	// Resume from a previously persisted checkpoint, if one exists for the
	// same file and has rows left to process.
	skip := 0
	cp, err := s.im.loadCheckpoint()
	if err != nil {
		s.log.Printf("error loading import checkpoint: %v", err)
	} else if cp.Filename == s.opt.Filename && cp.Processed > 0 {
		if cp.Processed >= numLines-1 {
			s.log.Printf("checkpoint for '%s' covers all %d rows; nothing to import", s.opt.Filename, numLines-1)
			close(s.subQueue)
			failed = false
			return nil
		}

		skip = cp.Processed
		if cp.Imported > 0 {
			s.im.incrementImportCount(cp.Imported)
		}
		s.log.Printf("resuming import of '%s' from line %d (previously imported %d)", s.opt.Filename, skip, cp.Imported)
	}

	rd := csv.NewReader(src)
	rd.Comma = delim
	// Allow rows with varying field counts so malformed rows can be skipped
	// individually (and recorded) instead of aborting the entire import.
	rd.FieldsPerRecord = -1

	// Read the header.
	csvHdr, err := rd.Read()
	if err != nil {
		s.log.Printf("error reading CSV header: %v", err)
		return err
	}

	hdrKeys := s.mapCSVHeaders(csvHdr, csvHeaders)
	// email is a required header.
	if _, ok := hdrKeys["email"]; !ok {
		s.log.Printf("'email' column not found in '%s'", s.opt.Filename)
		return errors.New("'email' column not found")
	}

	var (
		lnHdr    = len(hdrKeys)
		i        = 0
		lastSave = 0
	)
	for {
		i++

		// Check for the stop signal.
		select {
		case <-s.im.stop:
			failed = false
			s.persistCheckpoint(i)
			close(s.subQueue)
			s.log.Println("stop request received")
			s.logSummary()
			return nil
		default:
		}

		cols, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Any row-level parse error: skip the row and continue instead of
			// failing the entire batch.
			reason := fmt.Sprintf("CSV parse error: %v", err)
			s.recordError(i, reason)
			s.log.Printf("skipping line %d: %v", i, err)

			// A parse error can leave the reader positioned mid-record;
			// csv.Reader resynchronizes to the next line on the next Read.
			continue
		}

		// Persist the rows read so far periodically so the import can resume
		// after a crash/restart. The DB upserts are idempotent, so rows that
		// were queued but not yet committed may be reprocessed on resume safely.
		if i-lastSave >= commitBatchSize {
			s.persistCheckpoint(i)
			lastSave = i
		}

		// Skip rows already processed before a restart (checkpoint resume).
		if i <= skip {
			continue
		}

		lnCols := len(cols)
		if lnCols < lnHdr {
			reason := fmt.Sprintf("column count (%d) is fewer than the header count (%d)", lnCols, lnHdr)
			s.recordError(i, reason)
			s.log.Printf("skipping line %d. %s", i, reason)
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
			s.recordError(i, err.Error())
			s.log.Printf("skipping line %d: %v: %v", i, err, cols)
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

	close(s.subQueue)
	failed = false
	s.logSummary()

	return nil
}

// recordError records a skipped CSV row (line number and reason) for the
// end-of-import summary report and bumps the failed counter.
func (s *Session) recordError(line int, reason string) {
	s.failedCount++
	s.im.incrementFailedCount(1)
	if len(s.rowErrors) < maxRowErrors {
		s.rowErrors = append(s.rowErrors, RowError{Line: line, Reason: reason})
	}
}

// persistCheckpoint writes the current progress to the DB so the import can
// resume after a service restart.
func (s *Session) persistCheckpoint(processed int) {
	if err := s.im.saveCheckpoint(importCheckpoint{
		Filename:  s.opt.Filename,
		Processed: processed,
		Imported:  s.im.GetStats().Imported,
	}); err != nil {
		s.log.Printf("error persisting import checkpoint at line %d: %v", processed, err)
	}
}

// logSummary writes the end-of-import summary report (rows read, imported,
// failed and per-row error details) to the session log.
func (s *Session) logSummary() {
	st := s.im.GetStats()
	s.log.Printf("import summary for '%s': %d/%d rows imported, %d rows skipped", st.Name, st.Imported, st.Total, s.failedCount)
	if s.failedCount == 0 {
		return
	}

	s.log.Printf("skipped rows (line: reason):")
	for _, re := range s.rowErrors {
		s.log.Printf("  line %d: %s", re.Line, re.Reason)
	}
	if s.failedCount > len(s.rowErrors) {
		s.log.Printf("  ... and %d more skipped rows (capped at %d)", s.failedCount-len(s.rowErrors), maxRowErrors)
	}
}

// saveCheckpoint upserts the import resume checkpoint into the settings table.
func (im *Importer) saveCheckpoint(cp importCheckpoint) error {
	if im.db == nil {
		return nil
	}

	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	_, err = im.db.Exec(`INSERT INTO settings (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`, checkpointKey, b)
	return err
}

// loadCheckpoint loads a previously persisted import checkpoint. A missing
// checkpoint returns a zero-value checkpoint with no error.
func (im *Importer) loadCheckpoint() (importCheckpoint, error) {
	var cp importCheckpoint
	if im.db == nil {
		return cp, nil
	}

	var b []byte
	if err := im.db.QueryRow(`SELECT value FROM settings WHERE key = $1`, checkpointKey).Scan(&b); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cp, nil
		}
		return cp, err
	}

	return cp, json.Unmarshal(b, &cp)
}

// clearCheckpoint removes the import resume checkpoint after a successful import.
func (im *Importer) clearCheckpoint() error {
	if im.db == nil {
		return nil
	}

	_, err := im.db.Exec(`DELETE FROM settings WHERE key = $1`, checkpointKey)
	return err
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

// CountLines counts the number of line breaks in a stream. This does not
// distinguish between "blank" and non "blank" lines.
// Credit: https://stackoverflow.com/a/24563853
func CountLines(r io.Reader) (int, error) {
	var (
		buf      = make([]byte, 32*1024)
		count    = 0
		lineSep  = byte('\n')
		lastByte byte
	)

	for {
		c, err := r.Read(buf)
		if c > 0 {
			count += bytes.Count(buf[:c], []byte{lineSep})
			lastByte = buf[c-1]
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
	}

	if lastByte != 0 && lastByte != lineSep {
		count++
	}

	return count, nil
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
