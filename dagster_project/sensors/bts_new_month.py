"""
Sensor that polls the BTS PREZIP listing and requests a run for each
monthly file that has appeared since the last evaluation.

Dagster concept — sensors:
    A sensor runs on a schedule (minimum_interval_seconds) inside the Dagster
    daemon. Each evaluation either yields RunRequest objects (one per partition
    to kick off) or a SkipReason. Dagster deduplicates by run_key, so even if
    the sensor re-yields an already-submitted key it won't double-run.

Dagster concept — cursor:
    context.cursor is a string persisted between sensor evaluations. We store
    the set of already-seen partition keys as JSON so we don't re-yield months
    we've already submitted. Without this, every evaluation would yield all
    historically available months (harmless due to run_key dedup, but noisy).
"""

# from __future__ import annotations

import json
import re
from collections.abc import Iterator

import httpx
from bs4 import BeautifulSoup  # type: ignore[import-untyped]
from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

BTS_PREZIP_URL = 'https://transtats.bts.gov/PREZIP/'
_BTS_FILENAME_RE = re.compile(
    r'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_(\d{4})_(\d{1,2})\.zip'
)


def _available_bts_months() -> list[str]:
    """Scrape the PREZIP index and return partition keys for all available months."""
    res = httpx.get(BTS_PREZIP_URL, timeout=30, follow_redirects=True)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'lxml')
    keys: list[str] = []
    for link in soup.find_all('a', href=True):
        m = _BTS_FILENAME_RE.search(str(link['href']))
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            keys.append(f'{year}-{month:02d}-01')
    return sorted(keys)


@sensor(
    job_name='ingest_bts_month',  # matched by name in definitions
    minimum_interval_seconds=6 * 60 * 60,  # poll every 6 hours
    description='Polls transtats.bts.gov/PREZIP for new monthly releases.',
    # job='raw_bts_flights',
)
def bts_new_month_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest | SkipReason]:
    available = _available_bts_months()
    already_seen: set[str] = set(json.loads(context.cursor or '[]'))

    new_months = [key for key in available if key not in already_seen]

    if not new_months:
        yield SkipReason(
            f'no new BTS months. Latest available: {available[-1] if available else "none"}'
        )
        return

    for partition_key in new_months:
        context.log.info('New BTS month detected: %s', partition_key)
        yield RunRequest(
            run_key=partition_key,  # Dagster idempotency: same key = no duplicate run
            partition_key=partition_key,
        )

    # Advance cursor to the full known set so next evaluation only yields truly new months.
    context.update_cursor(json.dumps(available))
