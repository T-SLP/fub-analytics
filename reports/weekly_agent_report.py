#!/usr/bin/env python3
"""
Weekly Agent Performance Report

Generates a Google Sheets report with agent metrics:
- Stage progression: Offers Made, Contracts Sent, Under Contract, Closed
- Call metrics from FUB API

Can be run automatically via GitHub Actions or manually triggered.
Sends email reports on Monday (weekly) and Wednesday (midweek).
"""

import os
import sys
import argparse
import base64
import json
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

# Add project root to path for shared module imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared.constants import (
    EASTERN_TZ,
    INCLUDED_AGENTS,
    TRACKED_STAGES,
    PRE_OFFER_STAGES,
    CONNECTION_THRESHOLD_SECONDS,
)

# Load .env file from project root (for local development)
try:
    from dotenv import load_dotenv
    # Look for .env in parent directory (project root)
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass  # dotenv not required in production (GitHub Actions)

import psycopg2
import psycopg2.extras
import requests
import gspread
from google.oauth2.service_account import Credentials

# Configuration from environment
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
FUB_API_KEY = os.getenv("FUB_API_KEY")
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")  # JSON string
# Weekly Agent Report spreadsheet ID
WEEKLY_AGENT_SHEET_ID = "1MNnP-w70h7gv7NnpRxO6GZstq9m6wW5USAozzCev6gs"

# Email configuration
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_FROM = os.getenv("EMAIL_FROM", "travis@synergylandpartners.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail app password
EMAIL_TO = ["acquisitions@synergylandpartners.com", "dante@synergylandpartners.com"]
EMAIL_CC = ["travis@synergylandpartners.com"]


def get_date_range(days_back: int = None, previous_week: bool = False) -> tuple[datetime, datetime]:
    """
    Calculate the date range for the report.

    Modes:
    - previous_week=True: Returns the full previous week (Mon 00:00 to Sun 23:59 Eastern)
    - days_back specified: Simple "last N days" calculation
    - Default (auto): On Monday, returns previous week. Otherwise, returns current week so far.

    All modes use Monday-Sunday week boundaries in Eastern timezone to match FUB's standard report format.
    """
    # Use Eastern timezone for week boundaries
    now_eastern = datetime.now(EASTERN_TZ)

    if days_back is not None:
        # Legacy behavior: simple days-back calculation
        end_date = now_eastern
        start_date = end_date - timedelta(days=days_back)
        return start_date, end_date

    # weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = now_eastern.weekday()

    # Determine if we should report on previous week
    # On Monday (weekday=0), auto mode reports the previous full week
    use_previous_week = previous_week or (days_since_monday == 0)

    if use_previous_week:
        # Previous week: Monday through Sunday of last week
        # First, find the start of the current week (Monday 00:00 Eastern)
        start_of_current_week = now_eastern - timedelta(days=days_since_monday)
        start_of_current_week = start_of_current_week.replace(hour=0, minute=0, second=0, microsecond=0)

        # Previous week starts 7 days before current week
        start_date = start_of_current_week - timedelta(days=7)

        # Previous week ends at the end of Sunday (start of current week)
        end_date = start_of_current_week
    else:
        # Current week: Monday through now
        start_of_week = now_eastern - timedelta(days=days_since_monday)
        start_date = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now_eastern

    return start_date, end_date


def query_open_offers_metrics(fub_api_key: str) -> Dict[str, Dict[str, Any]]:
    """
    Query open offers and calculate follow-up metrics as percentages.

    Returns per-agent metrics:
    - total_open_offers: Total leads in "Offers Made" or "Contract Sent" (24+ hrs)
    - low_followup_pct: % of open offers with 0, 1, or 2 follow-up calls (need more outreach)
    - low_connection_pct: % of open offers with 0 or 1 connections (3+ min calls, need more engagement)

    Criteria for open offers:
    - Lead is currently in "ACQ - Offers Made" or "ACQ - Contract Sent" stage
    - Lead has been in this stage for 24+ hours (exclude immediate declines)

    Returns: {agent_name: {'total': N, 'low_followup_pct': 'X%', 'low_connection_pct': 'Y%'}, ...}
    """
    if not SUPABASE_DB_URL:
        return {}

    conn = psycopg2.connect(SUPABASE_DB_URL, sslmode='require')

    # Get all leads currently in open offer stages (24+ hours old)
    twenty_four_hours_ago = datetime.now(EASTERN_TZ) - timedelta(hours=24)

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                WITH latest_stages AS (
                    SELECT DISTINCT ON (person_id)
                        person_id,
                        stage_to,
                        changed_at,
                        COALESCE(
                            assigned_user_name,
                            raw_payload->>'assignedTo',
                            CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                        ) as agent
                    FROM stage_changes
                    ORDER BY person_id, changed_at DESC
                ),
                offer_dates AS (
                    SELECT DISTINCT ON (person_id)
                        person_id,
                        changed_at as offer_date
                    FROM stage_changes
                    WHERE stage_to = 'ACQ - Offers Made'
                    ORDER BY person_id, changed_at
                )
                SELECT
                    ls.person_id,
                    ls.agent,
                    od.offer_date
                FROM latest_stages ls
                JOIN offer_dates od ON ls.person_id = od.person_id
                WHERE ls.stage_to IN ('ACQ - Offers Made', 'ACQ - Contract Sent')
                  AND ls.changed_at < %s
            """, (twenty_four_hours_ago,))

            open_offers = []
            for row in cur.fetchall():
                open_offers.append({
                    'person_id': str(row['person_id']),
                    'agent': row['agent'],
                    'offer_date': row['offer_date'],
                })
    finally:
        conn.close()

    if not open_offers or not fub_api_key:
        return {}

    # Fetch calls to determine follow-up counts
    auth_string = base64.b64encode(f'{fub_api_key}:'.encode()).decode()

    earliest_offer = min(o['offer_date'] for o in open_offers)
    start_str = (earliest_offer - timedelta(days=1)).strftime('%Y-%m-%d')
    end_str = (datetime.now(EASTERN_TZ) + timedelta(days=1)).strftime('%Y-%m-%d')

    all_calls = []
    offset = 0
    limit = 100

    while True:
        try:
            response = requests.get(
                'https://api.followupboss.com/v1/calls',
                params={
                    'createdAfter': start_str,
                    'createdBefore': end_str,
                    'limit': limit,
                    'offset': offset
                },
                headers={
                    'Authorization': f'Basic {auth_string}',
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                calls = data.get('calls', [])
                all_calls.extend(calls)

                if len(calls) < limit:
                    break
                offset += limit
            else:
                break
        except Exception:
            break

    # Build maps of person_id -> calls and connections after offer date
    target_person_ids = {o['person_id'] for o in open_offers}
    offer_dates_map = {o['person_id']: o['offer_date'] for o in open_offers}

    calls_after_offer = {pid: 0 for pid in target_person_ids}
    connections_after_offer = {pid: 0 for pid in target_person_ids}

    for call in all_calls:
        person_id = call.get('personId')
        if not person_id:
            continue

        person_id_str = str(person_id)
        if person_id_str not in target_person_ids:
            continue

        # Only count outbound calls
        if call.get('isIncoming') == True:
            continue

        created = call.get('created')
        if not created:
            continue

        try:
            call_date = datetime.fromisoformat(created.replace('Z', '+00:00')).date()
            offer_date = offer_dates_map[person_id_str]
            if hasattr(offer_date, 'date'):
                offer_date = offer_date.date()

            if call_date >= offer_date:
                calls_after_offer[person_id_str] += 1
                # Count connections (calls >= threshold are real conversations, not voicemails)
                duration = call.get('duration', 0) or 0
                if duration >= CONNECTION_THRESHOLD_SECONDS:
                    connections_after_offer[person_id_str] += 1
        except Exception:
            continue

    # Calculate per-agent metrics
    agent_totals = {}
    agent_low_followup = {}
    agent_low_connection = {}

    for offer in open_offers:
        agent = offer['agent']
        person_id = offer['person_id']
        followup_count = calls_after_offer.get(person_id, 0)
        connection_count = connections_after_offer.get(person_id, 0)

        agent_totals[agent] = agent_totals.get(agent, 0) + 1

        # Count leads with 0, 1, or 2 follow-up calls (need more outreach)
        if followup_count <= 2:
            agent_low_followup[agent] = agent_low_followup.get(agent, 0) + 1

        # Count leads with 0 or 1 connections (need more engagement)
        if connection_count <= 1:
            agent_low_connection[agent] = agent_low_connection.get(agent, 0) + 1

    # Build result with percentages
    result = {}
    for agent in agent_totals:
        total = agent_totals[agent]
        low_followup = agent_low_followup.get(agent, 0)
        low_connection = agent_low_connection.get(agent, 0)

        low_followup_pct = round(low_followup / total * 100) if total > 0 else 0
        low_connection_pct = round(low_connection / total * 100) if total > 0 else 0

        result[agent] = {
            'total': total,
            'low_followup_count': low_followup,
            'low_followup_pct': f"{low_followup_pct}%",
            'low_connection_count': low_connection,
            'low_connection_pct': f"{low_connection_pct}%",
        }

    return result


def query_standardized_offer_metrics(start_date: datetime, end_date: datetime) -> Dict[str, int]:
    """
    Query standardized offer metrics with the following rules:

    1. Direct offers: Transition TO 'ACQ - Offers Made'
    2. Implicit offers: Transition TO 'ACQ - Offer Not Accepted' FROM a pre-offer stage
    3. Implicit offers: Transition TO 'ACQ - Contract Sent' FROM a pre-offer stage

    Pre-offer stages are stages where an offer hasn't been made yet:
    - ACQ - Qualified, ACQ - Needs Offer, Qualified Phase 2/3, etc.

    Post-offer stages (where an offer was already made) do NOT trigger implicit offers:
    - ACQ - Offers Made, ACQ - Contract Sent, ACQ - Under Contract, etc.

    24-hour deduplication: One lead can only count as 1 offer per 24-hour window.
    Multiple offers for the same lead are valid if 24+ hours apart.

    Returns: {agent_name: offer_count, ...}
    """
    if not SUPABASE_DB_URL:
        print("ERROR: SUPABASE_DB_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(SUPABASE_DB_URL, sslmode='require')
    offer_counts = {}

    # Pre-offer stages imported from shared.constants.PRE_OFFER_STAGES
    pre_offer_stages = PRE_OFFER_STAGES

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Query all offer events (direct + implicit) with 24-hour deduplication
            cur.execute("""
                WITH offer_events AS (
                    -- Direct offers: transition TO 'ACQ - Offers Made'
                    SELECT
                        person_id,
                        changed_at,
                        COALESCE(
                            assigned_user_name,
                            raw_payload->>'assignedTo',
                            CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                        ) as agent,
                        'direct' as offer_type
                    FROM stage_changes
                    WHERE changed_at >= %s
                      AND changed_at < %s
                      AND stage_to = 'ACQ - Offers Made'

                    UNION ALL

                    -- Implicit offers via Offer Not Accepted (skipped Offers Made stage)
                    -- Only counts when coming FROM a pre-offer stage
                    SELECT
                        person_id,
                        changed_at,
                        COALESCE(
                            assigned_user_name,
                            raw_payload->>'assignedTo',
                            CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                        ) as agent,
                        'implicit_not_accepted' as offer_type
                    FROM stage_changes
                    WHERE changed_at >= %s
                      AND changed_at < %s
                      AND stage_to = 'ACQ - Offer Not Accepted'
                      AND stage_from IN %s

                    UNION ALL

                    -- Implicit offers via Contract Sent (skipped Offers Made stage)
                    -- Only counts when coming FROM a pre-offer stage
                    SELECT
                        person_id,
                        changed_at,
                        COALESCE(
                            assigned_user_name,
                            raw_payload->>'assignedTo',
                            CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                        ) as agent,
                        'implicit_contract_sent' as offer_type
                    FROM stage_changes
                    WHERE changed_at >= %s
                      AND changed_at < %s
                      AND stage_to = 'ACQ - Contract Sent'
                      AND stage_from IN %s
                ),
                -- Add previous offer time for 24-hour deduplication
                offer_events_with_prev AS (
                    SELECT
                        person_id,
                        changed_at,
                        agent,
                        offer_type,
                        LAG(changed_at) OVER (PARTITION BY person_id ORDER BY changed_at) as prev_offer_time
                    FROM offer_events
                ),
                -- Filter to only include offers that are 24+ hours from previous offer
                deduped_offers AS (
                    SELECT *
                    FROM offer_events_with_prev
                    WHERE prev_offer_time IS NULL
                       OR EXTRACT(EPOCH FROM (changed_at - prev_offer_time)) / 3600 >= 24
                )
                SELECT
                    agent,
                    COUNT(*) as offer_count,
                    COUNT(*) FILTER (WHERE offer_type = 'direct') as direct_count,
                    COUNT(*) FILTER (WHERE offer_type = 'implicit_not_accepted') as implicit_not_accepted_count,
                    COUNT(*) FILTER (WHERE offer_type = 'implicit_contract_sent') as implicit_contract_sent_count
                FROM deduped_offers
                GROUP BY agent
                ORDER BY agent
            """, (start_date, end_date, start_date, end_date, pre_offer_stages, start_date, end_date, pre_offer_stages))

            for row in cur.fetchall():
                agent = row['agent']
                offer_counts[agent] = row['offer_count']

                # Debug logging
                print(f"  Offers for {agent}: {row['offer_count']} "
                      f"(direct: {row['direct_count']}, "
                      f"implicit_not_accepted: {row['implicit_not_accepted_count']}, "
                      f"implicit_contract_sent: {row['implicit_contract_sent_count']})")

    finally:
        conn.close()

    return offer_counts


def backfill_missing_stage_changes(start_date: datetime, end_date: datetime) -> int:
    """
    Backfill missing stage changes by cross-referencing FUB API with the database.

    The webhook server can miss events due to downtime or failures. This function
    queries FUB for people currently in tracked stages and inserts any missing
    stage transitions into the database, making the report self-healing.

    If a person jumped past intermediate tracked stages (e.g., Offers Made -> Under Contract),
    intermediate records are also inserted (e.g., Contract Sent) so all KPIs count correctly.

    Returns the number of backfilled records.
    """
    if not FUB_API_KEY or not SUPABASE_DB_URL:
        print("  WARNING: Cannot backfill - missing FUB_API_KEY or SUPABASE_DB_URL")
        return 0

    auth_string = base64.b64encode(f'{FUB_API_KEY}:'.encode()).decode()
    headers = {
        'Authorization': f'Basic {auth_string}',
        'Content-Type': 'application/json'
    }

    # Pipeline order for intermediate stage insertion
    STAGE_PIPELINE = [
        "ACQ - Offers Made",
        "ACQ - Contract Sent",
        "ACQ - Under Contract",
    ]

    # Convert report period to UTC ISO strings for comparison with FUB timestamps
    start_utc_str = start_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_utc_str = end_date.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    backfilled = 0
    conn = psycopg2.connect(SUPABASE_DB_URL, sslmode='require')

    try:
        for stage in TRACKED_STAGES:
            # Fetch people currently in this stage from FUB API (with pagination)
            people_in_stage = []
            offset = 0
            limit = 100

            while True:
                try:
                    response = requests.get(
                        'https://api.followupboss.com/v1/people',
                        params={
                            'stage': stage,
                            'limit': limit,
                            'offset': offset,
                            'fields': 'id,firstName,lastName,stage,assignedTo,assignedUserId,updated'
                        },
                        headers=headers,
                        timeout=30
                    )

                    if response.status_code == 200:
                        data = response.json()
                        people = data.get('people', [])
                        people_in_stage.extend(people)

                        if len(people) < limit:
                            break
                        offset += limit
                    else:
                        print(f"  WARNING: FUB API error {response.status_code} for stage '{stage}'")
                        break
                except Exception as e:
                    print(f"  WARNING: Error fetching people in '{stage}': {e}")
                    break

            # Check each person against the database
            for person in people_in_stage:
                fub_updated = person.get('updated', '')

                # Only backfill people updated within the report period
                if fub_updated < start_utc_str or fub_updated > end_utc_str:
                    continue

                person_id = str(person['id'])

                # Get latest stage from database
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("""
                        SELECT stage_to, changed_at
                        FROM stage_changes
                        WHERE person_id = %s
                        ORDER BY changed_at DESC
                        LIMIT 1
                    """, (person_id,))
                    row = cur.fetchone()

                db_latest_stage = row['stage_to'] if row else None

                # Skip if DB already shows this stage
                if db_latest_stage == stage:
                    continue

                # Skip if a backfill record already exists for this person/stage in this period
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    start_utc_naive = start_date.astimezone(timezone.utc).replace(tzinfo=None)
                    end_utc_naive = end_date.astimezone(timezone.utc).replace(tzinfo=None)
                    cur.execute("""
                        SELECT id FROM stage_changes
                        WHERE person_id = %s AND stage_to = %s
                          AND source = 'backfill'
                          AND changed_at >= %s AND changed_at < %s
                        LIMIT 1
                    """, (person_id, stage, start_utc_naive, end_utc_naive))
                    if cur.fetchone():
                        continue

                # Parse FUB updated timestamp as naive UTC for DB storage
                try:
                    fub_dt = datetime.fromisoformat(fub_updated.replace('Z', '+00:00'))
                    changed_at_naive = fub_dt.replace(tzinfo=None)
                except Exception:
                    changed_at_naive = datetime.now(timezone.utc).replace(tzinfo=None)

                # Extract agent info
                assigned_to = person.get('assignedTo', {})
                assigned_user_name = None
                assigned_user_id = person.get('assignedUserId')
                if assigned_to and isinstance(assigned_to, dict):
                    assigned_user_name = assigned_to.get('name')
                elif assigned_to and isinstance(assigned_to, str):
                    assigned_user_name = assigned_to

                first_name = person.get('firstName', 'Unknown')
                last_name = person.get('lastName', 'Unknown')

                # Check if intermediate pipeline stages need to be inserted
                # e.g., if DB shows "Offers Made" and FUB shows "Under Contract",
                # also insert "Contract Sent" so the Contracts Sent KPI counts correctly
                target_idx = next((i for i, s in enumerate(STAGE_PIPELINE) if s == stage), -1)
                db_idx = next((i for i, s in enumerate(STAGE_PIPELINE) if s == db_latest_stage), -1)

                prev_stage = db_latest_stage
                if db_idx >= 0 and target_idx > db_idx + 1:
                    # Insert intermediate stages with slightly earlier timestamps
                    for i in range(db_idx + 1, target_idx):
                        intermediate_stage = STAGE_PIPELINE[i]
                        offset_seconds = (target_idx - i) * 60  # 1 min before each subsequent stage
                        intermediate_dt = changed_at_naive - timedelta(seconds=offset_seconds)

                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO stage_changes (
                                    person_id, first_name, last_name, stage_from, stage_to,
                                    changed_at, received_at, source,
                                    assigned_user_id, assigned_user_name
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                person_id, first_name, last_name,
                                prev_stage, intermediate_stage,
                                intermediate_dt, datetime.now(timezone.utc).replace(tzinfo=None), 'backfill',
                                str(assigned_user_id) if assigned_user_id else None,
                                assigned_user_name
                            ))
                        backfilled += 1
                        print(f"    BACKFILLED (intermediate): {first_name} {last_name} (ID: {person_id}): "
                              f"{prev_stage} -> {intermediate_stage}")
                        prev_stage = intermediate_stage

                # Insert the final stage transition
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO stage_changes (
                            person_id, first_name, last_name, stage_from, stage_to,
                            changed_at, received_at, source,
                            assigned_user_id, assigned_user_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        person_id, first_name, last_name,
                        prev_stage, stage,
                        changed_at_naive, datetime.now(timezone.utc).replace(tzinfo=None), 'backfill',
                        str(assigned_user_id) if assigned_user_id else None,
                        assigned_user_name
                    ))
                conn.commit()
                backfilled += 1
                print(f"    BACKFILLED: {first_name} {last_name} (ID: {person_id}): "
                      f"{prev_stage} -> {stage} (agent: {assigned_user_name}, updated: {fub_updated})")

    except Exception as e:
        print(f"  ERROR during backfill: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    return backfilled


def query_stage_metrics(start_date: datetime, end_date: datetime) -> Dict[str, Dict[str, int]]:
    """
    Query Supabase for stage change metrics grouped by agent.
    Returns: {agent_name: {stage_name: count, ...}, ...}

    Note: For 'ACQ - Offers Made', this function uses standardized counting
    via query_standardized_offer_metrics() which includes:
    - Direct offers (transition TO 'ACQ - Offers Made')
    - Implicit offers (transition TO 'ACQ - Offer Not Accepted' or 'ACQ - Contract Sent'
      where they skipped the 'ACQ - Offers Made' stage)
    - 24-hour deduplication per lead
    """
    if not SUPABASE_DB_URL:
        print("ERROR: SUPABASE_DB_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(SUPABASE_DB_URL, sslmode='require')
    metrics = {}

    # First, get standardized offer counts
    print("  Querying standardized offer metrics...")
    offer_counts = query_standardized_offer_metrics(start_date, end_date)

    # Stages to query with existing logic (excluding 'ACQ - Offers Made')
    non_offer_stages = [s for s in TRACKED_STAGES if s != "ACQ - Offers Made"]

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Query stage changes for non-offer stages
            # Use COALESCE to fall back to raw_payload->>'assignedTo' for older records
            # where assigned_user_name wasn't populated.
            # For records before Dec 19, 2025 with no agent info, default to Madeleine Penales
            # (she was the only acquisition agent at that time)
            # Count DISTINCT person_id to avoid counting the same lead multiple times
            cur.execute("""
                SELECT
                    COALESCE(
                        assigned_user_name,
                        raw_payload->>'assignedTo',
                        CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                    ) as agent,
                    stage_to,
                    COUNT(DISTINCT person_id) as count
                FROM stage_changes
                WHERE changed_at >= %s
                  AND changed_at < %s
                  AND stage_to IN %s
                GROUP BY COALESCE(
                    assigned_user_name,
                    raw_payload->>'assignedTo',
                    CASE WHEN changed_at < '2025-12-19' THEN 'Madeleine Penales' ELSE 'Unassigned' END
                ), stage_to
                ORDER BY 1, 2
            """, (start_date, end_date, tuple(non_offer_stages)))

            for row in cur.fetchall():
                agent = row['agent']
                stage = row['stage_to']
                count = row['count']

                if agent not in metrics:
                    metrics[agent] = {s: 0 for s in TRACKED_STAGES}
                metrics[agent][stage] = count

        # Merge in the standardized offer counts
        for agent, offer_count in offer_counts.items():
            if agent not in metrics:
                metrics[agent] = {s: 0 for s in TRACKED_STAGES}
            metrics[agent]["ACQ - Offers Made"] = offer_count

    finally:
        conn.close()

    return metrics


def get_fub_users() -> Dict[str, int]:
    """
    Get mapping of FUB user names to IDs.
    Returns: {user_name: user_id, ...}
    """
    if not FUB_API_KEY:
        print("WARNING: FUB_API_KEY not set, skipping call metrics")
        return {}

    auth_string = base64.b64encode(f'{FUB_API_KEY}:'.encode()).decode()

    try:
        response = requests.get(
            'https://api.followupboss.com/v1/users',
            headers={
                'Authorization': f'Basic {auth_string}',
                'Content-Type': 'application/json'
            },
            timeout=30
        )

        if response.status_code == 200:
            data = response.json()
            users = data.get('users', [])
            return {u.get('name', ''): u.get('id') for u in users if u.get('name')}
        else:
            print(f"WARNING: Failed to fetch FUB users: {response.status_code}")
            return {}

    except Exception as e:
        print(f"WARNING: Error fetching FUB users: {e}")
        return {}


def query_call_metrics(start_date: datetime, end_date: datetime, user_ids: Dict[str, int]) -> Dict[str, Dict[str, Any]]:
    """
    Query FUB API for detailed call metrics per agent.
    Note: FUB API doesn't filter by userId, so we fetch all calls and group client-side.
    Returns: {agent_name: {calls: int, connected: int, conversations: int, talk_time_min: int}, ...}
    """
    if not FUB_API_KEY or not user_ids:
        return {}

    auth_string = base64.b64encode(f'{FUB_API_KEY}:'.encode()).decode()

    # Format dates for FUB API
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    # Fetch ALL calls for the date range (API doesn't filter by userId)
    all_calls = []
    offset = 0
    limit = 100

    print("  Fetching all calls from FUB API...")
    while True:
        try:
            response = requests.get(
                'https://api.followupboss.com/v1/calls',
                params={
                    'createdAfter': start_str,
                    'createdBefore': end_str,
                    'limit': limit,
                    'offset': offset
                },
                headers={
                    'Authorization': f'Basic {auth_string}',
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                calls = data.get('calls', [])
                all_calls.extend(calls)

                if len(calls) < limit:
                    break
                offset += limit
            else:
                print(f"WARNING: Failed to fetch calls: {response.status_code}")
                break
        except Exception as e:
            print(f"WARNING: Error fetching calls: {e}")
            break

    print(f"  Total calls fetched: {len(all_calls)}")

    # Build reverse lookup: userId -> userName
    user_id_to_name = {v: k for k, v in user_ids.items()}

    # Initialize metrics for all users
    call_metrics = {}
    for user_name in user_ids.keys():
        call_metrics[user_name] = {
            'outbound_calls': 0,
            'connected': 0,  # Outbound calls >= 1 min
            'outbound_connections': 0,  # Outbound calls >= CONNECTION_THRESHOLD_SECONDS (for Connection Rate)
            'conversations': 0,  # All calls (inbound + outbound) >= threshold (for Avg Call)
            'talk_time_min': 0,
            'long_call_durations': [],  # Track durations for calls >= threshold to calculate average
            'outbound_call_details': [],  # For multi-dial sequence analysis
            'unique_leads_dialed': set(),  # Track unique personIds for outbound calls
            'unique_leads_connected': set(),  # Track unique personIds for outbound connected calls
            'single_dial_calls': 0,
            'single_dial_answered': 0,  # Single dials that resulted in a connection
            'double_dial_sequences': 0,
            'double_dial_answered': 0,  # Double dial sequences that resulted in a connection
            'triple_dial_sequences': 0,
            'multi_dial_calls': 0,  # Total calls that are part of any 2x+ sequence
        }

    # Group calls by userId and calculate metrics
    # Our definitions (customized from FUB defaults):
    # - Outbound Calls = Outgoing calls only (isIncoming=False)
    # - Connections = Outbound calls with duration >= CONNECTION_THRESHOLD_SECONDS (real conversations, not voicemails)
    # - Connection Rate = Connections / Outbound Calls
    # - Talk Time = Total duration of all calls
    # - Avg Call = Average duration of calls >= CONNECTION_THRESHOLD_SECONDS
    for call in all_calls:
        call_user_id = call.get('userId')
        call_user_name = call.get('userName')
        is_outgoing = call.get('isIncoming') == False
        duration = call.get('duration', 0) or 0

        # Try to match by userId first, then by userName
        agent_name = None
        if call_user_id in user_id_to_name:
            agent_name = user_id_to_name[call_user_id]
        elif call_user_name in user_ids:
            agent_name = call_user_name

        if agent_name and agent_name in call_metrics:
            # Only count outgoing calls as "Outbound Calls"
            if is_outgoing:
                call_metrics[agent_name]['outbound_calls'] += 1

                # Connected = outbound calls >= 1 min
                if duration >= 60:
                    call_metrics[agent_name]['connected'] += 1

                # Outbound connections (for Connections metric & Connection Rate)
                if duration >= CONNECTION_THRESHOLD_SECONDS:
                    call_metrics[agent_name]['outbound_connections'] += 1

                # Collect call details for multi-dial sequence analysis
                to_number = call.get('toNumber')
                created = call.get('created')
                if to_number and created:
                    call_metrics[agent_name]['outbound_call_details'].append({
                        'to_number': to_number,
                        'created': created,
                        'duration': duration,
                    })

                # Track unique leads dialed and connected (outbound only)
                person_id = call.get('personId')
                if person_id:
                    call_metrics[agent_name]['unique_leads_dialed'].add(person_id)
                    if duration >= CONNECTION_THRESHOLD_SECONDS:
                        call_metrics[agent_name]['unique_leads_connected'].add(person_id)

            # Conversations = ANY call (inbound or outbound) >= threshold
            # Used for average call duration calculation
            if duration >= CONNECTION_THRESHOLD_SECONDS:
                call_metrics[agent_name]['conversations'] += 1
                call_metrics[agent_name]['long_call_durations'].append(duration)

            # Talk time includes all calls with duration
            if duration > 0:
                call_metrics[agent_name]['talk_time_min'] += duration

    # Convert total duration from seconds to minutes
    for agent_name in call_metrics:
        call_metrics[agent_name]['talk_time_min'] = call_metrics[agent_name]['talk_time_min'] // 60

    # Analyze multi-dial sequences for each agent
    # A sequence is consecutive calls to the same number with <= 2 min between each call
    for agent_name in call_metrics:
        calls = call_metrics[agent_name]['outbound_call_details']
        if not calls:
            # No calls - convert sets to counts and set defaults
            call_metrics[agent_name]['unique_leads_dialed'] = len(call_metrics[agent_name]['unique_leads_dialed'])
            call_metrics[agent_name]['unique_leads_connected'] = len(call_metrics[agent_name]['unique_leads_connected'])
            call_metrics[agent_name]['single_dial_calls'] = 0
            del call_metrics[agent_name]['outbound_call_details']
            continue

        # Sort calls by timestamp
        calls.sort(key=lambda x: x['created'])

        total_multi_dial_calls = 0
        single_dials = 0
        single_dial_answered = 0
        double_sequences = 0
        double_dial_answered = 0
        triple_sequences = 0

        i = 0
        while i < len(calls):
            current_number = calls[i]['to_number']
            sequence_calls = [calls[i]]
            sequence_length = 1

            j = i + 1
            while j < len(calls):
                # Parse timestamps and check time difference
                prev_time = datetime.fromisoformat(calls[j-1]['created'].replace('Z', '+00:00'))
                curr_time = datetime.fromisoformat(calls[j]['created'].replace('Z', '+00:00'))
                time_diff = (curr_time - prev_time).total_seconds()

                # Same number and within 2 minutes of previous call = part of sequence
                if calls[j]['to_number'] == current_number and time_diff <= 120:
                    sequence_calls.append(calls[j])
                    sequence_length += 1
                    j += 1
                else:
                    break

            # Check if any call in the sequence resulted in a real connection
            sequence_answered = any(c.get('duration', 0) >= CONNECTION_THRESHOLD_SECONDS for c in sequence_calls)

            # Count the sequence
            if sequence_length >= 3:
                triple_sequences += 1
                total_multi_dial_calls += sequence_length
            elif sequence_length == 2:
                double_sequences += 1
                total_multi_dial_calls += sequence_length
                if sequence_answered:
                    double_dial_answered += 1
            else:
                # Single dial
                single_dials += 1
                if sequence_answered:
                    single_dial_answered += 1

            # Move to next unprocessed call
            i = j if j > i + 1 else i + 1

        call_metrics[agent_name]['single_dial_calls'] = single_dials
        call_metrics[agent_name]['single_dial_answered'] = single_dial_answered
        call_metrics[agent_name]['double_dial_sequences'] = double_sequences
        call_metrics[agent_name]['double_dial_answered'] = double_dial_answered
        call_metrics[agent_name]['triple_dial_sequences'] = triple_sequences
        call_metrics[agent_name]['multi_dial_calls'] = total_multi_dial_calls

        # Convert unique leads sets to counts and clean up raw data
        call_metrics[agent_name]['unique_leads_dialed'] = len(call_metrics[agent_name]['unique_leads_dialed'])
        call_metrics[agent_name]['unique_leads_connected'] = len(call_metrics[agent_name]['unique_leads_connected'])
        del call_metrics[agent_name]['outbound_call_details']

    return call_metrics


def write_to_google_sheets(
    stage_metrics: Dict[str, Dict[str, int]],
    call_metrics: Dict[str, Dict[str, Any]],
    start_date: datetime,
    end_date: datetime,
    open_offers_metrics: Dict[str, Dict[str, Any]] = None
) -> str:
    """
    Write the report to Google Sheets with two tabs:
    - "Latest" tab: Current week's full report (overwritten each run)
    - "History" tab: Appends summary row per agent (most recent at top)

    Returns the URL to the spreadsheet.
    """
    if not GOOGLE_SHEETS_CREDENTIALS:
        print("ERROR: GOOGLE_SHEETS_CREDENTIALS not set")
        sys.exit(1)

    # Parse credentials from JSON string
    creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)

    # Open the spreadsheet
    spreadsheet = client.open_by_key(WEEKLY_AGENT_SHEET_ID)
    existing_tabs = [ws.title for ws in spreadsheet.worksheets()]

    # Prepare header row for History tab (includes week column)
    history_headers = [
        "Week Starting",
        "Agent",
        # KPIs
        "Offers Made",
        "Contracts Sent",
        "% Open Offers w/ <= 2 Dials After Offer",
        # Metrics
        "Talk Time (min)",
        "Outbound Calls",
        "Connections (3+ min)",
        "Connection Rate",
        "Unique Leads Dialed",
        "Unique Leads Connected",
        "Unique Lead Connection Rate",
        "Avg Call (min)",
        "Single Dial",
        "2x Dial",
        "% Open Offers w/ <=1 Connection After Offer",
        "Signed Contracts",
    ]

    # Collect agent data - filter to included agents only
    all_agents = set(stage_metrics.keys()) | set(call_metrics.keys())
    all_agents.discard('Unassigned')

    # Filter agents if INCLUDED_AGENTS is set
    if INCLUDED_AGENTS:
        all_agents = [a for a in sorted(all_agents) if a in INCLUDED_AGENTS]
    else:
        all_agents = sorted(all_agents)

    week_label = start_date.strftime('%Y-%m-%d')

    # Collect data for each agent
    agent_data = {}
    history_rows_to_add = []

    for agent in all_agents:
        stage_data = stage_metrics.get(agent, {})
        offers = stage_data.get("ACQ - Offers Made", 0)
        contracts_sent = stage_data.get("ACQ - Contract Sent", 0)
        signed_contracts = stage_data.get("ACQ - Under Contract", 0)

        call_data = call_metrics.get(agent, {})
        outbound_calls = call_data.get('outbound_calls', 0)
        unique_leads = call_data.get('unique_leads_dialed', 0)
        unique_leads_connected = call_data.get('unique_leads_connected', 0)
        connections = call_data.get('outbound_connections', 0)  # Outbound calls >= 3 min
        long_call_durations = call_data.get('long_call_durations', [])
        avg_call_min = round(sum(long_call_durations) / len(long_call_durations) / 60, 1) if long_call_durations else 0
        talk_time = call_data.get('talk_time_min', 0)
        # Connection Rate = outbound 3+ min calls / total outbound calls
        connection_rate = f"{round(connections / outbound_calls * 100)}%" if outbound_calls > 0 else "0%"
        # Unique Lead Connection Rate
        unique_lead_conn_rate = f"{round(unique_leads_connected / unique_leads * 100)}%" if unique_leads > 0 else "0%"
        single_dial = call_data.get('single_dial_calls', 0)
        single_dial_answered = call_data.get('single_dial_answered', 0)
        double_dial = call_data.get('double_dial_sequences', 0)
        double_dial_answered = call_data.get('double_dial_answered', 0)

        # Calculate answer rates for dial sequences
        single_dial_rate = round(single_dial_answered / single_dial * 100) if single_dial > 0 else 0
        double_dial_rate = round(double_dial_answered / double_dial * 100) if double_dial > 0 else 0

        # Format with answer rate in parentheses
        single_dial_with_rate = f"{single_dial} ({single_dial_rate}%)"
        double_dial_with_rate = f"{double_dial} ({double_dial_rate}%)"

        # Open offers follow-up metrics (as percentages)
        agent_offer_metrics = open_offers_metrics.get(agent, {}) if open_offers_metrics else {}
        low_followup_pct = agent_offer_metrics.get('low_followup_pct', '0%')
        low_conn_pct = agent_offer_metrics.get('low_connection_pct', '0%')

        # Store all metrics for this agent
        agent_data[agent] = {
            # KPIs
            'talk_time': talk_time,
            'offers': offers,
            'contracts_sent': contracts_sent,
            'low_followup_pct': low_followup_pct,
            # Metrics
            'outbound_calls': outbound_calls,
            'connections': connections,
            'connection_rate': connection_rate,
            'unique_leads': unique_leads,
            'unique_leads_connected': unique_leads_connected,
            'unique_lead_conn_rate': unique_lead_conn_rate,
            'avg_call_min': avg_call_min,
            'long_call_total_sec': sum(long_call_durations),  # For proper total avg calculation
            'long_call_count': len(long_call_durations),      # For proper total avg calculation
            'single_dial': single_dial,
            'single_dial_with_rate': single_dial_with_rate,
            'double_dial': double_dial,
            'double_dial_with_rate': double_dial_with_rate,
            'low_conn_pct': low_conn_pct,
            'signed_contracts': signed_contracts,
        }

        # Row for History tab (with week column)
        # Must match order of history_headers exactly
        history_row = [
            week_label, agent, offers, contracts_sent, low_followup_pct,
            talk_time, outbound_calls, connections, connection_rate,
            unique_leads, unique_leads_connected, unique_lead_conn_rate,
            avg_call_min, single_dial, double_dial, low_conn_pct, signed_contracts
        ]
        history_rows_to_add.append(history_row)

    # === Write to "Latest" tab ===
    print("  Writing to 'Latest' tab...")
    if "Latest" in existing_tabs:
        latest_ws = spreadsheet.worksheet("Latest")
        latest_ws.clear()
    else:
        latest_ws = spreadsheet.add_worksheet(title="Latest", rows=100, cols=20, index=0)

    # Calculate totals across all agents
    totals = {
        'offers': sum(agent_data[agent]['offers'] for agent in all_agents),
        'contracts_sent': sum(agent_data[agent]['contracts_sent'] for agent in all_agents),
        'talk_time': sum(agent_data[agent]['talk_time'] for agent in all_agents),
        'outbound_calls': sum(agent_data[agent]['outbound_calls'] for agent in all_agents),
        'connections': sum(agent_data[agent]['connections'] for agent in all_agents),
        'unique_leads': sum(agent_data[agent]['unique_leads'] for agent in all_agents),
        'unique_leads_connected': sum(agent_data[agent]['unique_leads_connected'] for agent in all_agents),
        'single_dial': sum(agent_data[agent]['single_dial'] for agent in all_agents),
        'double_dial': sum(agent_data[agent]['double_dial'] for agent in all_agents),
        'signed_contracts': sum(agent_data[agent]['signed_contracts'] for agent in all_agents),
        'long_call_total_sec': sum(agent_data[agent]['long_call_total_sec'] for agent in all_agents),
        'long_call_count': sum(agent_data[agent]['long_call_count'] for agent in all_agents),
    }
    # Calculate rates from totals (not averaging rates)
    totals['connection_rate'] = f"{round(totals['connections'] / totals['outbound_calls'] * 100)}%" if totals['outbound_calls'] > 0 else "0%"
    totals['unique_lead_conn_rate'] = f"{round(totals['unique_leads_connected'] / totals['unique_leads'] * 100)}%" if totals['unique_leads'] > 0 else "0%"
    # Avg Call (min) = average duration of 3+ min calls only (matches agent-level calculation)
    totals['avg_call_min'] = round(totals['long_call_total_sec'] / totals['long_call_count'] / 60, 1) if totals['long_call_count'] > 0 else 0
    # For dial sequences with answer rates, sum the counts (rates don't sum meaningfully)
    totals['single_dial_with_rate'] = totals['single_dial']
    totals['double_dial_with_rate'] = totals['double_dial']

    # Calculate overall open offers percentages from raw counts
    total_open_offers = sum(m['total'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    total_low_followup = sum(m['low_followup_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    total_low_conn = sum(m['low_connection_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    totals['low_followup_pct'] = f"{round(total_low_followup / total_open_offers * 100)}%" if total_open_offers > 0 else "0%"
    totals['low_conn_pct'] = f"{round(total_low_conn / total_open_offers * 100)}%" if total_open_offers > 0 else "0%"

    # Standards (expected minimums per agent)
    standards = {
        'offers': 18,
        'contracts_sent': 4,
        'low_followup_pct': "< 20%",  # Lower is better - target under 20%
        'talk_time': 220,
        'outbound_calls': 160,
        'connections': 30,
        'connection_rate': "-",
        'unique_leads': 50,
        'unique_leads_connected': "-",
        'unique_lead_conn_rate': "-",
        'avg_call_min': "-",
        'single_dial_with_rate': "-",
        'double_dial_with_rate': "-",
        'low_conn_pct': "< 75%",  # Lower is better - target under 75%
        'signed_contracts': 1,
    }

    # Build the Latest tab with exact formatting from user's template
    latest_rows = []

    # Row 1: Empty cell, then agent names, Total (Actuals), Standards (each AM)
    header_row = [""] + list(all_agents) + ["Total (Actuals)", "Standards (each AM)"]
    latest_rows.append(header_row)

    # Row 2: "KPIs" section header
    latest_rows.append(["KPIs"])

    # KPI rows
    kpi_metrics = [
        ("Offers Made", 'offers'),
        ("Contracts Sent", 'contracts_sent'),
        ("% Open Offers w/ <= 2 Dials After Offer", 'low_followup_pct'),
    ]
    for label, key in kpi_metrics:
        row = [label] + [agent_data[agent][key] for agent in all_agents] + [totals[key], standards[key]]
        latest_rows.append(row)

    # Empty row between sections
    latest_rows.append([""])

    # Row: "Metrics" section header
    latest_rows.append(["Metrics"])

    # Metrics rows (Talk Time moved here, 3x Dial removed)
    metric_items = [
        ("Talk Time (min)", 'talk_time'),
        ("Outbound Calls", 'outbound_calls'),
        ("Connections (3+ min)", 'connections'),
        ("Connection Rate", 'connection_rate'),
        ("Unique Leads Dialed", 'unique_leads'),
        ("Unique Leads Connected", 'unique_leads_connected'),
        ("Unique Lead Connection Rate", 'unique_lead_conn_rate'),
        ("Avg Call (min)", 'avg_call_min'),
        ("Single Dial", 'single_dial_with_rate'),
        ("2x Dial", 'double_dial_with_rate'),
        ("% Open Offers w/ <=1 Connection After Offer", 'low_conn_pct'),
        ("Signed Contracts", 'signed_contracts'),
    ]
    for label, key in metric_items:
        row = [label] + [agent_data[agent][key] for agent in all_agents] + [totals[key], standards[key]]
        latest_rows.append(row)

    # Empty row before metadata
    latest_rows.append([""])

    # Metadata rows
    # For previous week report: end_date is Monday 00:00, so subtract 1 day to show Sunday
    # For midweek report: end_date is the current time (e.g., Wed 2pm), so show date with time
    if end_date.hour == 0 and end_date.minute == 0:
        # Previous week report - end_date is midnight, so show previous day
        display_end_date = end_date - timedelta(days=1)
        date_range_str = f"{start_date.strftime('%Y-%m-%d')} to {display_end_date.strftime('%Y-%m-%d')}"
    else:
        # Midweek report - show the actual end date with time clarification
        display_end_date = end_date
        date_range_str = f"{start_date.strftime('%Y-%m-%d')} to {display_end_date.strftime('%Y-%m-%d')} (through {display_end_date.strftime('%I:%M %p')} ET)"
    latest_rows.append(["Report Generated:", datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')])
    latest_rows.append(["Date Range:", date_range_str])

    latest_ws.update(latest_rows, 'A1')

    # Apply formatting to match user's template
    num_agents = len(all_agents)
    agent_last_col = chr(65 + num_agents)  # B for 1 agent, C for 2 agents, etc.
    total_col = chr(65 + num_agents + 1)   # Total column
    standards_col = chr(65 + num_agents + 2)  # Standards column

    # Row 1: Agent names, Total, Standards - bold and centered
    latest_ws.format(f'B1:{standards_col}1', {
        'textFormat': {'bold': True},
        'horizontalAlignment': 'CENTER'
    })

    # Row 2: "KPIs" - bold
    latest_ws.format('A2', {'textFormat': {'bold': True}})

    # Row 6: Empty row (no formatting needed)

    # Row 7: "Metrics" - bold
    latest_ws.format('A7', {'textFormat': {'bold': True}})

    # Metric labels (column A, rows 3-5 and 8-19): right-aligned
    latest_ws.format('A3:A5', {'horizontalAlignment': 'RIGHT'})
    latest_ws.format('A8:A19', {'horizontalAlignment': 'RIGHT'})

    # Agent values (columns B to agent_last_col): centered
    latest_ws.format(f'B3:{agent_last_col}5', {'horizontalAlignment': 'CENTER'})
    latest_ws.format(f'B8:{agent_last_col}19', {'horizontalAlignment': 'CENTER'})

    # Total column: bold and centered
    latest_ws.format(f'{total_col}3:{total_col}5', {
        'textFormat': {'bold': True},
        'horizontalAlignment': 'CENTER'
    })
    latest_ws.format(f'{total_col}8:{total_col}19', {
        'textFormat': {'bold': True},
        'horizontalAlignment': 'CENTER'
    })

    # Standards column: centered with background color for visibility
    latest_ws.format(f'{standards_col}3:{standards_col}5', {
        'horizontalAlignment': 'CENTER',
        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
    })
    latest_ws.format(f'{standards_col}8:{standards_col}19', {
        'horizontalAlignment': 'CENTER',
        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
    })

    # Metadata rows: "Report Generated:" and "Date Range:" bold
    latest_ws.format('A21', {'textFormat': {'bold': True}})
    latest_ws.format('A22', {'textFormat': {'bold': True}})

    # === Write to "History" tab ===
    print("  Writing to 'History' tab...")
    if "History" in existing_tabs:
        history_ws = spreadsheet.worksheet("History")
        # Check if this week's data already exists (avoid duplicates)
        existing_data = history_ws.get_all_values()
        if existing_data and len(existing_data) > 1:
            # Check if this week is already in the history
            existing_weeks = [row[0] for row in existing_data[1:]]  # Skip header
            if week_label in existing_weeks:
                print(f"  Week {week_label} already exists in History, skipping...")
            else:
                # Insert new rows at row 2 (after header)
                history_ws.insert_rows(history_rows_to_add, row=2)
        else:
            # Empty sheet, just add header and data
            history_rows = [history_headers] + history_rows_to_add
            history_ws.update(history_rows, 'A1')
            history_ws.format('A1:R1', {'textFormat': {'bold': True}})
    else:
        # Create new History tab
        history_ws = spreadsheet.add_worksheet(title="History", rows=1000, cols=20, index=1)
        history_rows = [history_headers] + history_rows_to_add
        history_ws.update(history_rows, 'A1')
        history_ws.format('A1:R1', {'textFormat': {'bold': True}})

    return f"https://docs.google.com/spreadsheets/d/{WEEKLY_AGENT_SHEET_ID}/edit"


def generate_email_html(
    agent_data: Dict[str, Dict[str, Any]],
    totals: Dict[str, Any],
    standards: Dict[str, Any],
    all_agents: List[str],
    start_date: datetime,
    end_date: datetime,
    is_midweek: bool = False
) -> str:
    """
    Generate HTML email content matching the Google Sheet layout.
    """
    # Build HTML table
    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            table { border-collapse: collapse; margin: 20px 0; }
            th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: center; }
            th { background-color: #f5f5f5; font-weight: bold; }
            .section-header { background-color: #e8e8e8; font-weight: bold; text-align: left !important; }
            .metric-label { text-align: right !important; }
            .total-col { font-weight: bold; background-color: #f9f9f9; }
            .standards-col { background-color: #f0f0f0; }
            .empty-row { height: 10px; }
            .metadata { color: #666; font-size: 12px; margin-top: 20px; }
        </style>
    </head>
    <body>
    """

    # Add header
    if is_midweek:
        html += f"<h2>Midweek AM Metrics and KPIs Report</h2>"
    else:
        html += f"<h2>Weekly AM Metrics and KPIs Report</h2>"

    # For previous week report: end_date is Monday 00:00, so subtract 1 day to show Sunday
    # For midweek report: end_date is the current time (e.g., Wed 2pm), so just use the date
    if is_midweek:
        # Midweek report - show the actual end date with time
        display_end_date = end_date
        html += f"<p>Date Range: {start_date.strftime('%Y-%m-%d')} to {display_end_date.strftime('%Y-%m-%d')} (through {display_end_date.strftime('%I:%M %p')} ET)</p>"
    else:
        # Previous week report - end_date is midnight, so show previous day
        display_end_date = end_date - timedelta(days=1)
        html += f"<p>Date Range: {start_date.strftime('%Y-%m-%d')} to {display_end_date.strftime('%Y-%m-%d')}</p>"

    # Start table
    html += "<table>"

    # Header row
    html += "<tr><th></th>"
    for agent in all_agents:
        html += f"<th>{agent}</th>"
    html += "<th class='total-col'>Total (Actuals)</th>"
    html += "<th class='standards-col'>Standards (each AM)</th></tr>"

    # KPIs section header
    html += "<tr><td class='section-header' colspan='100%'>KPIs</td></tr>"

    # KPI rows
    kpi_metrics = [
        ("Offers Made", 'offers'),
        ("Contracts Sent", 'contracts_sent'),
        ("% Open Offers w/ <= 2 Dials After Offer", 'low_followup_pct'),
    ]
    for label, key in kpi_metrics:
        html += f"<tr><td class='metric-label'>{label}</td>"
        for agent in all_agents:
            html += f"<td>{agent_data[agent][key]}</td>"
        html += f"<td class='total-col'>{totals[key]}</td>"
        html += f"<td class='standards-col'>{standards[key]}</td></tr>"

    # Empty row
    html += "<tr class='empty-row'><td colspan='100%'></td></tr>"

    # Metrics section header
    html += "<tr><td class='section-header' colspan='100%'>Metrics</td></tr>"

    # Metrics rows
    metric_items = [
        ("Talk Time (min)", 'talk_time'),
        ("Outbound Calls", 'outbound_calls'),
        ("Connections (3+ min)", 'connections'),
        ("Connection Rate", 'connection_rate'),
        ("Unique Leads Dialed", 'unique_leads'),
        ("Unique Leads Connected", 'unique_leads_connected'),
        ("Unique Lead Connection Rate", 'unique_lead_conn_rate'),
        ("Avg Call (min)", 'avg_call_min'),
        ("Single Dial", 'single_dial_with_rate'),
        ("2x Dial", 'double_dial_with_rate'),
        ("% Open Offers w/ <=1 Connection After Offer", 'low_conn_pct'),
        ("Signed Contracts", 'signed_contracts'),
    ]
    for label, key in metric_items:
        html += f"<tr><td class='metric-label'>{label}</td>"
        for agent in all_agents:
            html += f"<td>{agent_data[agent][key]}</td>"
        html += f"<td class='total-col'>{totals[key]}</td>"
        html += f"<td class='standards-col'>{standards[key]}</td></tr>"

    html += "</table>"

    # Metadata
    html += f"""
    <div class='metadata'>
        <p>Report Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
    </div>
    </body>
    </html>
    """

    return html


def send_email_report(
    html_content: str,
    start_date: datetime,
    end_date: datetime,
    is_midweek: bool = False
) -> bool:
    """
    Send the report via email.
    Returns True if successful, False otherwise.
    """
    if not EMAIL_PASSWORD:
        print("WARNING: EMAIL_PASSWORD not set, skipping email")
        return False

    # Determine subject line
    if is_midweek:
        # Wednesday: use the current date
        subject_date = datetime.now(EASTERN_TZ).strftime('%B %d, %Y')
        subject = f"Midweek AM Metrics and KPIs Report - {subject_date}"
    else:
        # Monday: use the Saturday of the previous week (end_date is Sunday, so subtract 1 day)
        # end_date is the start of the current week (Monday 00:00), so Saturday is end_date - 2 days
        # Actually, for previous week report, end_date is Sunday 23:59 or Monday 00:00
        # We want the Saturday, which is 1 day before Sunday
        saturday_date = end_date - timedelta(days=1)
        if saturday_date.weekday() != 5:  # If not Saturday, find the previous Saturday
            days_since_saturday = (saturday_date.weekday() + 2) % 7
            saturday_date = saturday_date - timedelta(days=days_since_saturday)
        subject_date = saturday_date.strftime('%B %d, %Y')
        subject = f"Weekly AM Metrics and KPIs Report - For the week ending {subject_date}"

    try:
        # Create message
        message = MIMEMultipart("alternative")
        message["From"] = EMAIL_FROM
        message["To"] = ", ".join(EMAIL_TO)
        message["Cc"] = ", ".join(EMAIL_CC)
        message["Subject"] = subject

        # Attach HTML content
        html_part = MIMEText(html_content, "html")
        message.attach(html_part)

        # All recipients (To + Cc)
        all_recipients = EMAIL_TO + EMAIL_CC

        # Send email
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, all_recipients, message.as_string())

        print(f"Email sent successfully: {subject}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to send email: {e}")
        return False


def send_backfill_slack_alert(count: int):
    """Post a Slack alert when backfill detects missing webhook records."""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        health_url = f"{os.getenv('WEBHOOK_BASE_URL', 'https://fub-stage-tracker-production.up.railway.app')}/health"
        requests.post(SLACK_WEBHOOK_URL, json={
            "text": f"⚠️ Webhook gap detected — backfilled {count} missing stage change(s). Check Railway health: {health_url}"
        }, timeout=10)
    except Exception as e:
        print(f"WARNING: Failed to send backfill Slack alert: {e}")


def main():
    parser = argparse.ArgumentParser(description='Generate weekly agent performance report')
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Number of days to include in report (overrides week boundaries)'
    )
    parser.add_argument(
        '--previous-week',
        action='store_true',
        help='Report on the previous full week (Mon-Sun) instead of current week'
    )
    parser.add_argument(
        '--no-sheet',
        action='store_true',
        help='Skip writing to Google Sheets (for mid-week email-only reports)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print report to console without writing to Google Sheets'
    )
    parser.add_argument(
        '--send-email',
        action='store_true',
        help='Send report via email'
    )
    args = parser.parse_args()

    # Determine report type for logging
    now = datetime.now(timezone.utc)
    is_monday = now.weekday() == 0

    if args.days is not None:
        print(f"Generating agent report for the last {args.days} days...")
    elif args.previous_week:
        print("Generating agent report for previous week (Mon-Sun)...")
    elif is_monday:
        print("Monday detected - generating report for previous week (Mon-Sun)...")
    else:
        print("Generating agent report for current week (Mon-Sun) so far...")

    # Calculate date range
    start_date, end_date = get_date_range(args.days, args.previous_week)
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Backfill missing stage changes from FUB API before querying metrics
    print("Backfilling missing stage changes from FUB API...")
    backfill_count = backfill_missing_stage_changes(start_date, end_date)
    if backfill_count > 0:
        print(f"Backfilled {backfill_count} missing stage change(s)")
        send_backfill_slack_alert(backfill_count)
    else:
        print("No missing stage changes detected")

    # Get stage metrics from database
    print("Querying stage metrics from database...")
    stage_metrics = query_stage_metrics(start_date, end_date)
    print(f"Found metrics for {len(stage_metrics)} agents")

    # Get call metrics from FUB API
    print("Fetching FUB users...")
    fub_users = get_fub_users()
    print(f"Found {len(fub_users)} FUB users")

    print("Querying call metrics from FUB API...")
    call_metrics = query_call_metrics(start_date, end_date, fub_users)
    print(f"Retrieved call data for {len(call_metrics)} users")

    # Get open offers metrics (follow-up and connection percentages)
    print("Querying open offers follow-up metrics...")
    open_offers_metrics = query_open_offers_metrics(FUB_API_KEY)
    total_open = sum(m['total'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    total_low_followup = sum(m['low_followup_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    total_low_conn = sum(m['low_connection_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
    print(f"Found {total_open} open offers, {total_low_followup} with <=2 follow-up dials, {total_low_conn} with <=1 connection")

    def print_report_to_console(title_suffix=""):
        """Helper to print report data to console."""
        print("\n" + "=" * 100)
        print(f"AGENT PERFORMANCE REPORT{title_suffix}")
        print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print("=" * 100)

        # Print header rows (split into two lines for readability)
        print(f"\n{'Agent':<25} {'Offers':<7} {'Contracts':<10} {'Under K':<8} {'Closed':<7} {'Outbound':<9} {'Leads':<7} {'Connected':<10} {'Convos':<7} {'ConnRate':<9} {'Talk(m)':<8} {'Avg(m)':<7} {'Single':<7} {'2x':<4} {'3x':<4}")
        print("-" * 140)

        all_agents = set(stage_metrics.keys()) | set(call_metrics.keys())
        for agent in sorted(all_agents):
            stage_data = stage_metrics.get(agent, {})
            offers = stage_data.get("ACQ - Offers Made", 0)
            contracts = stage_data.get("ACQ - Contract Sent", 0)
            under_contract = stage_data.get("ACQ - Under Contract", 0)
            closed = stage_data.get("Closed", 0) + stage_data.get("ACQ - Closed Won", 0)

            call_data = call_metrics.get(agent, {})
            outbound_calls = call_data.get('outbound_calls', 0)
            unique_leads = call_data.get('unique_leads_dialed', 0)
            unique_connected = call_data.get('unique_leads_connected', 0)
            connections = call_data.get('outbound_connections', 0)
            long_call_durations = call_data.get('long_call_durations', [])
            avg_call_min = round(sum(long_call_durations) / len(long_call_durations) / 60, 1) if long_call_durations else 0
            talk_time = call_data.get('talk_time_min', 0)
            connection_rate = f"{round(connections / outbound_calls * 100)}%" if outbound_calls > 0 else "0%"
            single_dial = call_data.get('single_dial_calls', 0)
            double_seq = call_data.get('double_dial_sequences', 0)
            triple_seq = call_data.get('triple_dial_sequences', 0)

            # Truncate long agent names for display
            display_name = agent[:24] if len(agent) > 24 else agent
            print(f"{display_name:<25} {offers:<7} {contracts:<10} {under_contract:<8} {closed:<7} {outbound_calls:<9} {unique_leads:<7} {unique_connected:<10} {connections:<7} {connection_rate:<9} {talk_time:<8} {avg_call_min:<7} {single_dial:<7} {double_seq:<4} {triple_seq:<4}")

    # Prepare agent data for email (same logic as write_to_google_sheets)
    def prepare_email_data():
        """Prepare data structures needed for email."""
        all_agents_set = set(stage_metrics.keys()) | set(call_metrics.keys())
        all_agents_set.discard('Unassigned')
        if INCLUDED_AGENTS:
            agents_list = [a for a in sorted(all_agents_set) if a in INCLUDED_AGENTS]
        else:
            agents_list = sorted(all_agents_set)

        agent_data = {}
        for agent in agents_list:
            stage_data = stage_metrics.get(agent, {})
            offers = stage_data.get("ACQ - Offers Made", 0)
            contracts_sent = stage_data.get("ACQ - Contract Sent", 0)
            signed_contracts = stage_data.get("ACQ - Under Contract", 0)

            call_data = call_metrics.get(agent, {})
            outbound_calls = call_data.get('outbound_calls', 0)
            unique_leads = call_data.get('unique_leads_dialed', 0)
            unique_leads_connected = call_data.get('unique_leads_connected', 0)
            connections = call_data.get('outbound_connections', 0)  # Outbound calls >= 3 min
            long_call_durations = call_data.get('long_call_durations', [])
            avg_call_min = round(sum(long_call_durations) / len(long_call_durations) / 60, 1) if long_call_durations else 0
            talk_time = call_data.get('talk_time_min', 0)
            connection_rate = f"{round(connections / outbound_calls * 100)}%" if outbound_calls > 0 else "0%"
            unique_lead_conn_rate = f"{round(unique_leads_connected / unique_leads * 100)}%" if unique_leads > 0 else "0%"
            single_dial = call_data.get('single_dial_calls', 0)
            single_dial_answered = call_data.get('single_dial_answered', 0)
            double_dial = call_data.get('double_dial_sequences', 0)
            double_dial_answered = call_data.get('double_dial_answered', 0)

            single_dial_rate = round(single_dial_answered / single_dial * 100) if single_dial > 0 else 0
            double_dial_rate = round(double_dial_answered / double_dial * 100) if double_dial > 0 else 0
            single_dial_with_rate = f"{single_dial} ({single_dial_rate}%)"
            double_dial_with_rate = f"{double_dial} ({double_dial_rate}%)"

            agent_offer_metrics = open_offers_metrics.get(agent, {}) if open_offers_metrics else {}
            low_followup_pct = agent_offer_metrics.get('low_followup_pct', '0%')
            low_conn_pct = agent_offer_metrics.get('low_connection_pct', '0%')

            agent_data[agent] = {
                'offers': offers,
                'contracts_sent': contracts_sent,
                'low_followup_pct': low_followup_pct,
                'talk_time': talk_time,
                'outbound_calls': outbound_calls,
                'connections': connections,
                'connection_rate': connection_rate,
                'unique_leads': unique_leads,
                'unique_leads_connected': unique_leads_connected,
                'unique_lead_conn_rate': unique_lead_conn_rate,
                'avg_call_min': avg_call_min,
                'single_dial_with_rate': single_dial_with_rate,
                'double_dial_with_rate': double_dial_with_rate,
                'low_conn_pct': low_conn_pct,
                'signed_contracts': signed_contracts,
                'long_call_total_sec': sum(long_call_durations),
                'long_call_count': len(long_call_durations),
            }

        # Calculate totals
        totals = {
            'offers': sum(agent_data[a]['offers'] for a in agents_list),
            'contracts_sent': sum(agent_data[a]['contracts_sent'] for a in agents_list),
            'talk_time': sum(agent_data[a]['talk_time'] for a in agents_list),
            'outbound_calls': sum(agent_data[a]['outbound_calls'] for a in agents_list),
            'connections': sum(agent_data[a]['connections'] for a in agents_list),
            'unique_leads': sum(agent_data[a]['unique_leads'] for a in agents_list),
            'unique_leads_connected': sum(agent_data[a]['unique_leads_connected'] for a in agents_list),
            'single_dial': sum(int(agent_data[a]['single_dial_with_rate'].split()[0]) for a in agents_list),
            'double_dial': sum(int(agent_data[a]['double_dial_with_rate'].split()[0]) for a in agents_list),
            'signed_contracts': sum(agent_data[a]['signed_contracts'] for a in agents_list),
            'long_call_total_sec': sum(agent_data[a]['long_call_total_sec'] for a in agents_list),
            'long_call_count': sum(agent_data[a]['long_call_count'] for a in agents_list),
        }
        totals['connection_rate'] = f"{round(totals['connections'] / totals['outbound_calls'] * 100)}%" if totals['outbound_calls'] > 0 else "0%"
        totals['unique_lead_conn_rate'] = f"{round(totals['unique_leads_connected'] / totals['unique_leads'] * 100)}%" if totals['unique_leads'] > 0 else "0%"
        totals['avg_call_min'] = round(totals['long_call_total_sec'] / totals['long_call_count'] / 60, 1) if totals['long_call_count'] > 0 else 0
        totals['single_dial_with_rate'] = totals['single_dial']
        totals['double_dial_with_rate'] = totals['double_dial']

        total_open_offers = sum(m['total'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
        total_low_followup_count = sum(m['low_followup_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
        total_low_conn_count = sum(m['low_connection_count'] for m in open_offers_metrics.values()) if open_offers_metrics else 0
        totals['low_followup_pct'] = f"{round(total_low_followup_count / total_open_offers * 100)}%" if total_open_offers > 0 else "0%"
        totals['low_conn_pct'] = f"{round(total_low_conn_count / total_open_offers * 100)}%" if total_open_offers > 0 else "0%"

        standards = {
            'offers': 18,
            'contracts_sent': 4,
            'low_followup_pct': "< 20%",
            'talk_time': 220,
            'outbound_calls': 160,
            'connections': 30,
            'connection_rate': "-",
            'unique_leads': 50,
            'unique_leads_connected': "-",
            'unique_lead_conn_rate': "-",
            'avg_call_min': "-",
            'single_dial_with_rate': "-",
            'double_dial_with_rate': "-",
            'low_conn_pct': "< 75%",
            'signed_contracts': 1,
        }

        return agent_data, totals, standards, agents_list

    if args.dry_run:
        # Dry run - print to console only
        print_report_to_console(" (DRY RUN)")
        print("\n(Dry run - no data written to Google Sheets)")
    elif args.no_sheet:
        # Mid-week email-only run - no Google Sheet tab
        print_report_to_console(" (MID-WEEK)")
        print("\n(Mid-week report - no Google Sheet tab created)")
        if args.send_email:
            print("Preparing and sending email report...")
            agent_data, totals, standards, agents_list = prepare_email_data()
            html_content = generate_email_html(agent_data, totals, standards, agents_list, start_date, end_date, is_midweek=True)
            send_email_report(html_content, start_date, end_date, is_midweek=True)
    else:
        # Full report - write to Google Sheets
        print("Writing report to Google Sheets...")
        sheet_url = write_to_google_sheets(stage_metrics, call_metrics, start_date, end_date, open_offers_metrics)
        print(f"\nReport created successfully!")
        print(f"View report: {sheet_url}")
        if args.send_email:
            print("Preparing and sending email report...")
            agent_data, totals, standards, agents_list = prepare_email_data()
            html_content = generate_email_html(agent_data, totals, standards, agents_list, start_date, end_date, is_midweek=False)
            send_email_report(html_content, start_date, end_date, is_midweek=False)


if __name__ == '__main__':
    main()
