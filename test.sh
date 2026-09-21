#!/usr/bin/env bash
#
# test.sh - subscriber importer (internal/subimporter + cmd/import.go) tests.
#
# Usage:
#   ./test.sh unit       Run Go unit tests (build + vet + go test). No DB needed.
#   ./test.sh fixtures   Generate CSV/ZIP fixtures for manual testing.
#   ./test.sh manual     Guided manual API tests for partial failure, progress,
#                        streaming and resume-after-restart.
#   ./test.sh all        Run unit tests, generate fixtures, print manual steps.
#
# Manual test prerequisites:
#   - A running listmonk instance (default http://localhost:9000).
#   - An admin basic-auth token / cookie. Export these before `manual`:
#       export LM_URL=http://localhost:9000
#       export LM_AUTH='Authorization: Basic <base64(user:pass)>'
#       export LM_LIST_ID=1
#       export LM_PSQL='postgres://listmonk:listmonk@localhost:9432/listmonk?sslmode=disable'
#
set -euo pipefail

cd "$(dirname "$0")"

export GOCACHE="${GOCACHE:-/tmp/gocache-listmonk}"
FIXDIR="/tmp/listmonk-import-test"

LM_URL="${LM_URL:-http://localhost:9000}"
# e.g. export LM_AUTH="Authorization: Basic $(printf 'admin:admin' | base64)"
LM_AUTH="${LM_AUTH:-}"

# ---------------------------------------------------------------------------
# 1. Automated unit tests
# ---------------------------------------------------------------------------
run_unit() {
	echo "==> go build ./..."
	GOCACHE="$GOCACHE" go build ./...

	echo "==> go vet (changed packages)"
	GOCACHE="$GOCACHE" go vet ./internal/subimporter/ ./cmd/

	echo "==> go test ./internal/subimporter/"
	GOCACHE="$GOCACHE" go test -v -count=1 ./internal/subimporter/

	echo "==> UNIT TESTS PASSED"
}

# ---------------------------------------------------------------------------
# 2. Generate manual-test fixtures
# ---------------------------------------------------------------------------
make_fixtures() {
	mkdir -p "$FIXDIR"

	# 2.1 Mixed file: 6 good rows (one with invalid-but-tolerated attributes
	#     JSON) and 2 bad rows (invalid e-mail, too few columns).
	cat > "$FIXDIR/mixed.csv" <<CSV
email,name,attributes
alice@example.com,Alice,"{""city"":""Shanghai""}"
bob@example.com,Bob,"{""city"":""Beijing""}"
this-is-not-an-email,BadEmail,{}
carol@example.com,Carol,{not-json
dave@example.com,Dave,{}
only,two
erin@example.com,Erin,"{""city"":""Shenzhen""}"
frank@example.com,,{}
CSV

	# 2.2 All-good file.
	cat > "$FIXDIR/good.csv" <<CSV
email,name,attributes
good1@example.com,Good One,{}
good2@example.com,Good Two,"{""plan"":""pro""}"
good3@example.com,Good Three,{}
CSV

	# 2.3 Large streaming fixture (200k rows) to verify constant memory/disk.
	echo "email,name,attributes" > "$FIXDIR/big.csv"
	for i in $(seq 1 200000); do
		echo "bulk${i}@example.com,Bulk ${i},\"{\\\"n\\\":${i}}\""
	done >> "$FIXDIR/big.csv"

	# 2.4 ZIP containing a CSV plus a junk non-CSV file (must be ignored).
	mkdir -p "$FIXDIR/zipsrc"
	cp "$FIXDIR/good.csv" "$FIXDIR/zipsrc/subscribers.csv"
	echo "not a csv" > "$FIXDIR/zipsrc/readme.txt"
	( cd "$FIXDIR/zipsrc" && rm -f "$FIXDIR/subscribers.zip" && zip -q "$FIXDIR/subscribers.zip" subscribers.csv readme.txt )

	echo "==> fixtures written to $FIXDIR"
	ls -lh "$FIXDIR"
}

# ---------------------------------------------------------------------------
# 3. Guided manual API tests
# ---------------------------------------------------------------------------
manual_steps() {
	cat <<STEPS

===========================================================================
MANUAL TEST CHECKLIST  (run against: $LM_URL)
===========================================================================

Set credentials first, for example:
  export LM_AUTH='Authorization: Basic '\$(printf 'admin:admin' | base64)
  export LM_LIST_ID=1
  export LM_PSQL='postgres://listmonk:listmonk@localhost:9432/listmonk?sslmode=disable'

--- A. Partial failure mode (skip bad rows + line/reason summary) ---------
1) Upload the mixed file:
  curl -s -H "\$LM_AUTH" \\
    -F 'params={"mode":"subscribe","lists":['\$LM_LIST_ID'],"delim":",","subscription_status":"confirmed"}' \\
    -F "file=@$FIXDIR/mixed.csv;type=text/csv" \\
    "\$LM_URL/api/import/subscribers"

2) Poll stats until "status":"finished" (watch total/imported/failed/percent/eta):
  watch -n1 "curl -s -H \"\$LM_AUTH\" \$LM_URL/api/import/subscribers"

   Expected: total=8, imported=6 (carol's bad attributes JSON is tolerated,
   the row is still imported), failed=2 (invalid e-mail on data line 3 and
   the short "only,two" row on data line 6). percent reaches 100 and eta
   counts down to 0.

3) Inspect the summary report (line numbers + reasons):
  curl -s -H "\$LM_AUTH" \$LM_URL/api/import/subscribers/logs
   Expected log lines:
     "import summary for 'mixed.csv': 6/8 rows imported, 2 rows skipped"
     "line 3: Invalid e-mail" and "line 6: column count (2) ..."

4) Verify subscribers alice/bob/dave/erin/frank exist, this-is-not-an-email doesn't:
  psql "\$LM_PSQL" -c "SELECT email FROM subscribers WHERE email LIKE '%@example.com' ORDER BY email;"

--- B. Percent + ETA (start timestamp + speed) ----------------------------
5) Upload the 200k-row file and poll rapidly:
  curl -s -H "\$LM_AUTH" \\
    -F 'params={"mode":"subscribe","lists":['\$LM_LIST_ID'],"delim":",","subscription_status":"confirmed"}' \\
    -F "file=@$FIXDIR/big.csv;type=text/csv" \\
    "\$LM_URL/api/import/subscribers"
  while sleep 1; do curl -s -H "\$LM_AUTH" \$LM_URL/api/import/subscribers; echo; done
   Expected: started_at non-zero, percent monotonically rises to 100,
   eta (seconds remaining) falls roughly linearly.

--- C. Streaming upload (no full temp copy) -------------------------------
6) While step 5 is running, check the listmonk process does NOT hold a growing
   second temp copy of the upload (only the HTTP server's multipart spool):
  ls -lh /tmp/listmonk* 2>/dev/null || echo "no listmonk temp files (expected)"
   Memory should stay flat:
  ps -o rss= -p \$(pgrep -f './listmonk' | head -1)

--- D. ZIP streaming ------------------------------------------------------
7) Upload the ZIP; only subscribers.csv is processed, readme.txt ignored:
  curl -s -H "\$LM_AUTH" \\
    -F 'params={"mode":"subscribe","lists":['\$LM_LIST_ID'],"delim":",","subscription_status":"confirmed"}' \\
    -F "file=@$FIXDIR/subscribers.zip;type=application/zip" \\
    "\$LM_URL/api/import/subscribers"
   Expected: 3 imported, no files extracted to /tmp.

--- E. Resume after restart (checkpoint in DB) ----------------------------
8) Re-upload big.csv, then KILL listmonk (kill -9) mid-import.
   Check the persisted checkpoint:
  psql "\$LM_PSQL" -c "SELECT value FROM settings WHERE key='import.checkpoint';"
   Expected: {"filename":"big.csv","processed":N,"imported":M} with N > 0.

9) Restart listmonk and re-upload the SAME big.csv. Check the logs:
  curl -s -H "\$LM_AUTH" \$LM_URL/api/import/subscribers/logs
   Expected: "resuming import of 'big.csv' from line N (previously imported M)".
   Stats keep counting from M; previously imported subscribers are upserted
   (idempotent, no duplicates):
  psql "\$LM_PSQL" -c "SELECT count(*) FROM subscribers WHERE email LIKE 'bulk%@example.com';"
   Expected count = 200000 at the end.

10) After the resumed import finishes, the checkpoint must be cleared:
  psql "\$LM_PSQL" -c "SELECT count(*) FROM settings WHERE key='import.checkpoint';"
   Expected: 0.

--- F. Stop keeps the checkpoint ------------------------------------------
11) Start another import, call DELETE to stop it, verify checkpoint remains:
  curl -s -X DELETE -H "\$LM_AUTH" \$LM_URL/api/import/subscribers
  psql "\$LM_PSQL" -c "SELECT value FROM settings WHERE key='import.checkpoint';"

--- G. Cleanup -------------------------------------------------------------
  psql "\$LM_PSQL" -c "DELETE FROM subscribers WHERE email LIKE '%@example.com'; DELETE FROM settings WHERE key='import.checkpoint';"
  rm -rf "$FIXDIR"
===========================================================================
STEPS
}

case "${1:-unit}" in
	unit)     run_unit ;;
	fixtures) make_fixtures ;;
	manual)   make_fixtures; manual_steps ;;
	all)      run_unit; make_fixtures; manual_steps ;;
	*)
		echo "usage: $0 {unit|fixtures|manual|all}"
		exit 1
		;;
esac
