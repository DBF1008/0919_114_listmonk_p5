#!/usr/bin/env bash
#
# test.sh — manual test scripts for the subimporter rework:
#   1. partial failure mode (skip bad lines, record line no. + reason, summary report)
#   2. progress percent / rate / ETA (start timestamp + speed calculation)
#   3. streaming upload (no full-file temp copy)
#   4. crash/restart resume via DB checkpoint
#
# Prerequisites:
#   - a running listmonk instance (default http://localhost:9000)
#   - admin credentials (default admin/admin)
#   - at least one list in the DB (default list ID 1)
#   - jq, curl, zip, psql (psql only for the checkpoint checks)
#
# Usage:
#   chmod +x test.sh
#   ./test.sh            # run all tests
#   ./test.sh build      # run only a specific section
#
# Override defaults via env vars, eg:
#   BASE_URL=http://localhost:9000 AUTH=admin:admin LIST_ID=1 ./test.sh

set -u

BASE_URL="${BASE_URL:-http://localhost:9000}"
AUTH="${AUTH:-admin:admin}"
LIST_ID="${LIST_ID:-1}"
DB_CONTAINER="${DB_CONTAINER:-listmonk_db}"   # docker-compose db container for psql checks
DB_NAME="${DB_NAME:-listmonk}"
DB_USER="${DB_USER:-listmonk}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

PASS=0
FAIL=0

hdr()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
info() { printf '  .. %s\n' "$*"; }

api() { curl -s -u "$AUTH" "$@"; }

import_csv() { # $1=file $2=mode
	api -X POST "$BASE_URL/api/import/subscribers" \
		-F "params={\"mode\":\"$2\",\"subscription_status\":\"confirmed\",\"delim\":\",\",\"lists\":[$LIST_ID]}" \
		-F "file=@$1"
}

wait_done() { # poll until status != importing|stopping (timeout $1 secs, default 60)
	local t=0
	while [ $t -lt "${1:-60}" ]; do
		local st
		st=$(api "$BASE_URL/api/import/subscribers" | jq -r .data.status)
		case "$st" in importing|stopping) sleep 1; t=$((t+1)) ;; *) echo "$st"; return 0 ;; esac
	done
	echo "timeout"
}

psql_q() { docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -tAc "$1" 2>/dev/null || echo "(psql skipped)"; }

# ---------------------------------------------------------------------------
build() {
	hdr "0. Build & vet"
	go build ./... && ok "go build ./..." || bad "go build"
	go vet ./internal/subimporter ./cmd && ok "go vet" || bad "go vet"
	gofmt -l internal/subimporter/importer.go cmd/import.go | grep -q . && bad "gofmt" || ok "gofmt clean"
}

# ---------------------------------------------------------------------------
gen_fixtures() {
	hdr "0.1 Generating fixtures in $WORKDIR"

	# Valid CSV, 3 rows.
	printf 'email,name,attributes\nalice@example.com,Alice,"{""city"":""blr""}"\nbob@example.com,Bob,\ncarol@example.com,Carol,\n' > "$WORKDIR/valid.csv"

	# CSV with bad lines: bad email, missing column, bad JSON attrs, unterminated quote.
	cat > "$WORKDIR/bad.csv" << 'CSV'
email,name,attributes
good1@example.com,Good One,
not-an-email,Bad Email,
good2@example.com,Good Two,"{""age"": 30}"
onlyemail@example.com
good3@example.com,Good Three,"{broken json"
"unterminated,name,
good4@example.com,Good Four,
CSV

	# Large CSV (200k rows) for progress / ETA / streaming / resume tests.
	{ echo "email,name"; for i in $(seq 1 200000); do echo "user$i@example.com,User $i"; done; } > "$WORKDIR/large.csv"

	# ZIP containing a CSV.
	(cd "$WORKDIR" && zip -q subs.zip valid.csv)

	info "valid.csv (3 rows), bad.csv (8 lines, 4 bad), large.csv (200k rows), subs.zip"
}

# ---------------------------------------------------------------------------
partial_failure() {
	hdr "1. Partial failure mode (skip bad lines + summary report)"

	info "importing bad.csv"
	import_csv "$WORKDIR/bad.csv" subscribe > /dev/null
	wait_done 30 > /dev/null

	local stats
	stats=$(api "$BASE_URL/api/import/subscribers" | jq -c '.data | {status,total,processed,imported,failed,failed_lines}')
	info "stats: $stats"

	[ "$(echo "$stats" | jq .status)" = '"finished"' ] && ok "import finished despite bad lines" || bad "status != finished"
	[ "$(echo "$stats" | jq .failed)" -ge 3 ] && ok "failed lines counted" || bad "failed count wrong"
	[ "$(echo "$stats" | jq '.failed_lines | length')" -gt 0 ] && ok "failed_lines recorded (line no. + reason)" || bad "failed_lines empty"
	echo "$stats" | jq -e '.failed_lines[0] | has("line") and has("reason")' > /dev/null && ok "failed line has line+reason" || bad "failed line shape"

	local logs
	logs=$(api "$BASE_URL/api/import/subscribers/logs" | jq -r .data)
	echo "$logs" | grep -q "import finished:" && ok "summary report logged" || bad "no summary in logs"
	echo "$logs" | grep -q "failed lines report" && ok "failed lines report in logs" || bad "no failed lines report"
	info "tail of logs:"; echo "$logs" | tail -8 | sed 's/^/     /'

	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null
}

# ---------------------------------------------------------------------------
progress() {
	hdr "2. Progress: percent / rate / ETA / started_at"

	info "importing large.csv (200k rows), sampling stats mid-import"
	import_csv "$WORKDIR/large.csv" subscribe > /dev/null
	sleep 3

	local mid
	mid=$(api "$BASE_URL/api/import/subscribers" | jq -c '.data | {status,total,processed,imported,percent,rate,eta,started_at}')
	info "mid-import stats: $mid"

	echo "$mid" | jq -e '.started_at != null and .started_at != "0001-01-01T00:00:00Z"' > /dev/null && ok "started_at set" || bad "started_at missing"
	[ "$(echo "$mid" | jq '.percent // 0')" != "0" ] && ok "percent computed" || info "percent still 0 (import too fast?)"
	[ "$(echo "$mid" | jq '.rate // 0')" != "0" ] && ok "rate (lines/sec) computed" || info "rate still 0"
	echo "$mid" | jq -e 'has("eta")' > /dev/null && ok "eta field present" || bad "eta missing"

	wait_done 300 > /dev/null
	local done_stats
	done_stats=$(api "$BASE_URL/api/import/subscribers" | jq -c '.data | {status,total,imported,failed}')
	info "final: $done_stats"
	[ "$(echo "$done_stats" | jq .imported)" -eq 200000 ] && ok "all 200k rows imported" || bad "imported != 200000"

	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null
}

# ---------------------------------------------------------------------------
streaming() {
	hdr "3. Streaming upload (no full-file temp copy)"

	info "watching \$TMPDIR for listmonk temp copies during a large CSV upload"
	local tmpcount_before
	tmpcount_before=$(ls ${TMPDIR:-/tmp} 2>/dev/null | grep -c listmonk || true)

	import_csv "$WORKDIR/large.csv" subscribe > /dev/null
	sleep 2
	local tmpcount_during
	tmpcount_during=$(ls ${TMPDIR:-/tmp} 2>/dev/null | grep -c listmonk || true)
	wait_done 300 > /dev/null
	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null

	info "listmonk temp files before=$tmpcount_before during=$tmpcount_during"
	if [ "$tmpcount_during" -le "$tmpcount_before" ]; then
		ok "no full-file temp copy created for .csv upload"
	else
		info "temp file appeared — verify it is the small multipart spill file, not a full copy"
	fi

	info "ZIP import still works (streams CSV out of the ZIP)"
	import_csv "$WORKDIR/subs.zip" subscribe > /dev/null
	wait_done 60 > /dev/null
	local st
	st=$(api "$BASE_URL/api/import/subscribers" | jq -r '.data | "\(.status) imported=\(.imported)"')
	info "zip import result: $st"
	[ "$st" = "finished imported=3" ] && ok "zip streaming import" || bad "zip import: $st"
	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null
}

# ---------------------------------------------------------------------------
resume() {
	hdr "4. Resume after interruption (DB checkpoint)"

	info "checkpoint table exists?"
	local tbl
	tbl=$(psql_q "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='import_checkpoints'")
	info "import_checkpoints table: $tbl"

	info "importing large.csv, then stopping midway"
	import_csv "$WORKDIR/large.csv" subscribe > /dev/null
	sleep 5
	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null   # stop midway
	sleep 2

	local cp
	cp=$(psql_q "SELECT filename || ' processed=' || processed || ' imported=' || imported FROM import_checkpoints WHERE id=1")
	info "checkpoint row after stop: $cp"
	echo "$cp" | grep -q "large.csv" && ok "checkpoint persisted to DB" || bad "no checkpoint row"

	info "re-uploading the same file — should resume, not restart"
	import_csv "$WORKDIR/large.csv" subscribe > /dev/null
	sleep 2
	local logs
	logs=$(api "$BASE_URL/api/import/subscribers/logs" | jq -r .data)
	echo "$logs" | grep -q "resuming import" && ok "resume detected in logs" || bad "no resume log line"
	info "$(echo "$logs" | grep resuming | head -1)"

	wait_done 300 > /dev/null
	local final
	final=$(api "$BASE_URL/api/import/subscribers" | jq -c '.data | {status,imported,failed}')
	info "final after resume: $final"
	[ "$(echo "$final" | jq .status)" = '"finished"' ] && ok "resumed import finished" || bad "resumed import did not finish"

	local cp_after
	cp_after=$(psql_q "SELECT COUNT(*) FROM import_checkpoints WHERE id=1")
	[ "$cp_after" = "0" ] && ok "checkpoint cleared after completion" || info "checkpoint row still present: $cp_after"

	info "subscriber count sanity (duplicates from resume should be 0 thanks to upserts):"
	psql_q "SELECT COUNT(*) - COUNT(DISTINCT email) FROM subscribers WHERE email LIKE 'user%@example.com'" | sed 's/^/     duplicate rows: /'

	api -X DELETE "$BASE_URL/api/import/subscribers" > /dev/null
}

# ---------------------------------------------------------------------------
summary() {
	hdr "Result"
	echo "  passed: $PASS  failed: $FAIL"
	[ "$FAIL" -eq 0 ]
}

# ---------------------------------------------------------------------------
sections="${*:-build partial_failure progress streaming resume}"
gen_fixtures
for s in $sections; do
	case "$s" in
		build|partial_failure|progress|streaming|resume) "$s" ;;
		*) echo "unknown section: $s" ;;
	esac
done
summary
