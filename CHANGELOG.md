# CrowdScription changelog

Each entry is one notable addition or change. Newest first.

## 2026-05-13
- **Verify & Sign Off workflow** — `POST /api/worklist/{id}/verify` freezes the formatted text, appends the radiologist signature block, sets `worklist_status='verified'`, and stamps `verified_at`/`verified_by`. New “Verify & Sign Off” button on worklist detail.
- **Print / PDF view** — `GET /worklist/{id}/print` returns a clean HTML report page; the browser's built-in Save-as-PDF handles the export. No new server deps.
- **Per-doctor signature blocks at finalisation** — `formatter.finalize_report_text()` appends the radiologist's signature from the `Radiologist` table during verify.
- **Auto-learn on typist save** — Every save derives token-pair corrections, skips ones already in `WordReplacement` / `custom_corrections.json` / previously rejected, and queues new ones as pending `CorrectionFeedback`.
- **Approve / reject inbox on /learning** — Pending corrections grouped by pair with occurrence count and doctor list. Approve → live `WordReplacement` rule. Reject → silenced forever.
- **Regex phrase-replace with preview** — Custom corrections support a `regex` flag. Preview button on /learning runs the pattern against recent transcriptions and shows before/after snippets.
- **Local report templates library** — `ReportTemplate` table, `mine-templates` CLI seeds it from Karisma (6,337 entries). `GET /api/templates` exposes the library to the SpeechMike agent and UI.
- **SpeechMike agent fetches local templates** — `fetch_remote_templates()` + `get_templates_snapshot()` in the agent's formatter, called on startup.
- **Kestral extent fetcher (pluggable)** — `_fetch_external_extent(handle)` resolves Karisma's external referral GUIDs to bytes via filesystem roots configured by `KARISMA_EXTENT_ROOTS`.
- **30-second new-transcriptions watcher** — `scripts/watch_new_transcriptions.py` polls the local SQLite for fresh rows, no Karisma traffic.
- **Worklist auto-done sync** — Polling service re-checks Karisma for ready items; any that have been dictated outside CrowdScription are moved to verified.
- **Latest changes feed on home page** — This panel.
