"""Incremental Salesforce -> PostgreSQL sync for Account records.

Pulls only records changed since the last successful run, upserts them
into Postgres keyed on the Salesforce Id (so reruns never create
duplicates), and retries transient API failures with backoff.
"""
import logging
import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sf_sync")

SF_DOMAIN = os.environ["SF_DOMAIN"].rstrip("/")
SF_CLIENT_ID = os.environ["SF_CLIENT_ID"]
SF_CLIENT_SECRET = os.environ["SF_CLIENT_SECRET"]
SF_API_VERSION = os.getenv("SF_API_VERSION", "v62.0")
DATABASE_URL = os.environ["DATABASE_URL"]

OBJECT_NAME = "Account"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RETRYABLE = {429, 500, 502, 503, 504}


class SalesforceClient:
    """Thin REST client: OAuth client-credentials login, retries, pagination."""

    def __init__(self):
        self.session = requests.Session()

    def authenticate(self):
        resp = self.session.post(
            f"{SF_DOMAIN}/services/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
            },
            timeout=30,
        )
        resp.raise_for_status()
        self.session.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"
        log.info("Authenticated with Salesforce")

    def get(self, url, params=None, max_attempts=5):
        """GET with retry + exponential backoff for transient failures."""
        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                reason = f"network error: {exc}"
            else:
                if resp.status_code == 401:
                    # Token expired mid-run: log in again and retry.
                    log.info("Token expired, re-authenticating")
                    self.authenticate()
                    continue
                if resp.status_code < 400:
                    return resp.json()
                if resp.status_code not in RETRYABLE:
                    resp.raise_for_status()  # 400/403/404: retrying won't help
                reason = f"HTTP {resp.status_code}"
            wait = 2 ** attempt
            log.warning("Attempt %d/%d failed (%s), retrying in %ds", attempt, max_attempts, reason, wait)
            time.sleep(wait)
        raise RuntimeError(f"Giving up after {max_attempts} attempts: {url}")

    def query_pages(self, soql):
        """Yield pages of records, following Salesforce's nextRecordsUrl."""
        data = self.get(f"{SF_DOMAIN}/services/data/{SF_API_VERSION}/query", params={"q": soql})
        yield data["records"]
        while not data["done"]:
            data = self.get(f"{SF_DOMAIN}{data['nextRecordsUrl']}")
            yield data["records"]


SCHEMA = """
CREATE TABLE IF NOT EXISTS sf_account (
    sf_id        TEXT PRIMARY KEY,
    name         TEXT,
    industry     TEXT,
    sf_modstamp  TIMESTAMPTZ NOT NULL,
    synced_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS sync_state (
    object_name    TEXT PRIMARY KEY,
    last_modstamp  TIMESTAMPTZ NOT NULL
);
"""

# Idempotent upsert. The WHERE clause stops an older copy of a record
# from overwriting a newer one if pages ever arrive out of order.
UPSERT = """
INSERT INTO sf_account (sf_id, name, industry, sf_modstamp)
VALUES (%s, %s, %s, %s)
ON CONFLICT (sf_id) DO UPDATE SET
    name = EXCLUDED.name,
    industry = EXCLUDED.industry,
    sf_modstamp = EXCLUDED.sf_modstamp,
    synced_at = now()
WHERE sf_account.sf_modstamp <= EXCLUDED.sf_modstamp
"""

SAVE_WATERMARK = """
INSERT INTO sync_state (object_name, last_modstamp) VALUES (%s, %s)
ON CONFLICT (object_name) DO UPDATE SET last_modstamp = EXCLUDED.last_modstamp
"""


def parse_sf_time(value):
    # Salesforce returns e.g. 2026-10-02T18:04:11.000+0000
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")


def to_soql_time(dt):
    # SOQL datetime literals are unquoted, e.g. 2026-10-02T18:04:11Z
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run():
    sf = SalesforceClient()
    sf.authenticate()

    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute(SCHEMA)
        conn.commit()

        row = conn.execute(
            "SELECT last_modstamp FROM sync_state WHERE object_name = %s", (OBJECT_NAME,)
        ).fetchone()
        watermark = row[0] if row else EPOCH
        log.info("Pulling %s records changed since %s", OBJECT_NAME, watermark.isoformat())

        soql = (
            "SELECT Id, Name, Industry, SystemModstamp FROM Account "
            f"WHERE SystemModstamp > {to_soql_time(watermark)} "
            "ORDER BY SystemModstamp"
        )

        total = 0
        for records in sf.query_pages(soql):
            if not records:
                continue
            rows = [
                (r["Id"], r["Name"], r.get("Industry"), parse_sf_time(r["SystemModstamp"]))
                for r in records
            ]
            page_max = max(r[3] for r in rows)
            # Data and checkpoint commit together: a crash mid-run resumes
            # from the last fully saved page, never skipping records.
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(UPSERT, rows)
                conn.execute(SAVE_WATERMARK, (OBJECT_NAME, page_max))
            total += len(rows)
            log.info("Upserted %d records (watermark now %s)", len(rows), page_max.isoformat())

        log.info("Done. %d records processed.", total)


if __name__ == "__main__":
    run()
