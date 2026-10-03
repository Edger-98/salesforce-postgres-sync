# Salesforce to PostgreSQL Sync

A small incremental sync that pulls Salesforce Account records into PostgreSQL.

## How it works

1. **Authenticates** with Salesforce using the OAuth 2.0 client credentials flow (a Connected App).
2. **Pulls only what changed** since the last run, using a `SystemModstamp` watermark stored in Postgres, instead of re-pulling everything. This keeps API usage low, which matters because Salesforce limits API calls per day.
3. **Upserts** each record keyed on its Salesforce Id, so running the sync twice never creates duplicates.
4. **Retries** transient failures (timeouts, 429 rate limits, 5xx errors) with exponential backoff, and re-authenticates if the token expires mid-run.
5. **Commits data and the watermark together**, page by page, so a crash resumes from the last saved page without skipping records.

## Run it

```bash
docker compose up -d                 # local Postgres
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                 # then fill in your Salesforce values
python sync.py                       # run it twice: the second run finds nothing new
```

## Design choices

- **Polling vs events:** polling on a schedule is simple and easy to reason about. For near real-time needs I would switch to Salesforce Change Data Capture feeding a queue.
- **Salesforce is the source of truth.** Postgres holds a copy for reporting and processing.
- **Older updates never overwrite newer ones:** the upsert only applies if the incoming record is at least as new as the stored one.

## What I would add next

- Change Data Capture events into a queue (e.g. AWS SQS) with a dead-letter queue for failed messages
- A scheduled run (cron or AWS EventBridge) and alerting on failures
- Bulk API 2.0 for very large initial loads
- A periodic reconciliation job to catch any drift
