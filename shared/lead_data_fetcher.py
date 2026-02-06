#!/usr/bin/env python3
"""
Lead Data Fetcher - Utility to pull all available data for FUB leads

This module provides a reusable utility for fetching and bundling all available
data for leads from Follow Up Boss. It can fetch data for:
- A single lead by person_id
- All leads in a specific stage
- Multiple leads by a list of person_ids

The bundled data can then be used for LLM analysis, reporting, or other purposes.
"""

import os
import sys
import base64
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

# Load .env file from project root
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

import requests

# Configuration
FUB_API_KEY = os.getenv("FUB_API_KEY")
FUB_SUBDOMAIN = os.getenv("FUB_SUBDOMAIN", "synergylandgroup")

# Custom field mappings - FUB API field names to friendly names
MARKET_VALUE_FIELDS = {
    'customMarketTotalParcelValue': 'county_value',      # County's valuation (may include structures)
    'customMarketValueEstimate': 'li_comp_value',        # Land Insights estimate (land only)
}

# Additional custom fields of interest
PROPERTY_FIELDS = {
    'customParcelCounty': 'county',
    'customParcelState': 'state',
    'customAcreage': 'acreage',
    'customRoadFrontageFT': 'road_frontage',
    'customMarketValueEstimateConfidence': 'li_confidence',
    'customCampaignID': 'campaign_id',
    'customWhoPushedTheLead': 'lead_pusher',
}

# All custom fields to request from FUB API (must be explicitly requested)
CUSTOM_FIELDS_TO_FETCH = [
    'customCampaignID',
    'customWhoPushedTheLead',
    'customParcelCounty',
    'customParcelState',
    'customAcreage',
    'customRoadFrontageFT',
    'customMarketTotalParcelValue',
    'customMarketValueEstimate',
    'customMarketValueEstimateConfidence',
]


class LeadDataFetcher:
    """
    Fetches and bundles all available data for leads from Follow Up Boss.

    Usage:
        fetcher = LeadDataFetcher()

        # Fetch single lead
        bundle = fetcher.fetch_lead(person_id=12345)

        # Fetch all leads in a stage
        bundles = fetcher.fetch_leads_by_stage("ACQ - Qualified", limit=50)

        # Fetch multiple leads
        bundles = fetcher.fetch_leads([12345, 67890, 11111])
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or FUB_API_KEY
        if not self.api_key:
            raise ValueError("FUB_API_KEY not provided and not found in environment")

        self.base_url = 'https://api.followupboss.com/v1'
        self.session = requests.Session()
        self.session.headers.update(self._get_auth_headers())

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for FUB API"""
        auth_string = base64.b64encode(f'{self.api_key}:'.encode()).decode()
        return {
            'Authorization': f'Basic {auth_string}',
            'Content-Type': 'application/json'
        }

    def _make_request(self, endpoint: str, params: Optional[Dict] = None, max_retries: int = 3) -> Optional[Dict]:
        """Make a request to the FUB API with proactive rate limit handling.

        FUB rate limits: 200 requests per 10 second sliding window.
        Response headers include X-RateLimit-Remaining to track usage.
        """
        retry_count = 0
        base_delay = 2.0  # Start with 2 second delay for retries

        while retry_count <= max_retries:
            try:
                response = self.session.get(
                    f'{self.base_url}/{endpoint}',
                    params=params,
                    timeout=30
                )

                # Proactively check rate limit headers and pause before hitting limit
                # FUB allows 200 requests per 10 second window
                remaining = response.headers.get('X-RateLimit-Remaining')
                window = response.headers.get('X-RateLimit-Window', '10')
                if remaining is not None:
                    remaining = int(remaining)
                    if remaining < 10:
                        # Getting very close to limit - pause to let window reset
                        pause_time = float(window)  # Wait for full window reset
                        print(f"  Rate limit low ({remaining} remaining), pausing {pause_time:.1f}s...")
                        time.sleep(pause_time)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    return None
                elif response.status_code == 429:
                    # Rate limited - wait and retry with exponential backoff
                    retry_count += 1
                    if retry_count > max_retries:
                        print(f"  Warning: Rate limit exceeded for {endpoint} after {max_retries} retries")
                        return None
                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                    print(f"  Rate limited on {endpoint}, waiting {delay:.1f}s (attempt {retry_count}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    print(f"  Warning: API request failed for {endpoint}: {response.status_code}")
                    return None
            except Exception as e:
                print(f"  Warning: Exception during API request for {endpoint}: {e}")
                return None

        return None

    def _fetch_person(self, person_id: int) -> Optional[Dict]:
        """Fetch the person record with custom fields"""
        # Build fields parameter - must explicitly request custom fields from FUB API
        base_fields = ['id', 'firstName', 'lastName', 'stage', 'assignedTo', 'source',
                       'created', 'lastActivity', 'tags', 'phones', 'emails', 'addresses']
        all_fields = base_fields + CUSTOM_FIELDS_TO_FETCH
        return self._make_request(f'people/{person_id}', {'fields': ','.join(all_fields)})

    def _fetch_notes(self, person_id: int) -> List[Dict]:
        """Fetch all notes for a person"""
        result = self._make_request('notes', {'personId': person_id, 'limit': 100})
        return result.get('notes', []) if result else []

    def _fetch_calls(self, person_id: int) -> List[Dict]:
        """Fetch all calls for a person"""
        result = self._make_request('calls', {'personId': person_id, 'limit': 100})
        return result.get('calls', []) if result else []

    def _fetch_events(self, person_id: int) -> List[Dict]:
        """Fetch all events for a person"""
        result = self._make_request('events', {'personId': person_id, 'limit': 100})
        return result.get('events', []) if result else []

    def _fetch_tasks(self, person_id: int) -> List[Dict]:
        """Fetch all tasks for a person"""
        result = self._make_request('tasks', {'personId': person_id, 'limit': 100})
        return result.get('tasks', []) if result else []

    def _fetch_emails(self, person_id: int) -> List[Dict]:
        """Fetch all emails for a person"""
        result = self._make_request('emails', {'personId': person_id, 'limit': 100})
        return result.get('emails', []) if result else []

    def _fetch_text_messages(self, person_id: int) -> List[Dict]:
        """Fetch all text messages for a person (metadata only - content is hidden by FUB)"""
        result = self._make_request('textMessages', {'personId': person_id, 'limit': 100})
        # FUB API returns 'textmessages' (lowercase) in the response
        return result.get('textmessages', result.get('textMessages', [])) if result else []

    def _extract_market_values(self, person: Dict) -> Dict[str, Any]:
        """Extract market value fields from person record"""
        values = {}
        for fub_field, friendly_name in MARKET_VALUE_FIELDS.items():
            raw_value = person.get(fub_field)
            if raw_value:
                try:
                    values[friendly_name] = float(raw_value)
                except (ValueError, TypeError):
                    values[friendly_name] = raw_value
            else:
                values[friendly_name] = None
        return values

    def _extract_property_info(self, person: Dict) -> Dict[str, Any]:
        """Extract property-related custom fields from person record"""
        info = {}
        for fub_field, friendly_name in PROPERTY_FIELDS.items():
            value = person.get(fub_field)
            if friendly_name == 'acreage' and value:
                try:
                    info[friendly_name] = float(value)
                except (ValueError, TypeError):
                    info[friendly_name] = value
            elif friendly_name == 'road_frontage' and value:
                try:
                    info[friendly_name] = float(value)
                except (ValueError, TypeError):
                    info[friendly_name] = value
            else:
                info[friendly_name] = value
        return info

    def _compute_summary(self, calls: List[Dict], notes: List[Dict], events: List[Dict], text_messages: List[Dict] = None) -> Dict[str, Any]:
        """Compute summary statistics from the fetched data"""
        text_messages = text_messages or []

        summary = {
            'total_notes': len(notes),
            'total_calls': len(calls),
            'total_events': len(events),
            'total_talk_time_seconds': 0,
            'outbound_calls': 0,
            'inbound_calls': 0,
            'connected_calls': 0,  # Calls with duration > 60 seconds
            'last_call_date': None,
            'last_note_date': None,
            # Text message stats
            'total_texts': len(text_messages),
            'outbound_texts': 0,
            'inbound_texts': 0,
            'text_response_rate': None,
            'last_text_date': None,
        }

        for call in calls:
            duration = call.get('duration', 0) or 0
            summary['total_talk_time_seconds'] += duration

            if call.get('isIncoming'):
                summary['inbound_calls'] += 1
            else:
                summary['outbound_calls'] += 1

            if duration >= 60:
                summary['connected_calls'] += 1

            call_date = call.get('created')
            if call_date and (not summary['last_call_date'] or call_date > summary['last_call_date']):
                summary['last_call_date'] = call_date

        for note in notes:
            note_date = note.get('created')
            if note_date and (not summary['last_note_date'] or note_date > summary['last_note_date']):
                summary['last_note_date'] = note_date

        # Text message stats
        for text in text_messages:
            if text.get('isIncoming'):
                summary['inbound_texts'] += 1
            else:
                summary['outbound_texts'] += 1

            text_date = text.get('created')
            if text_date and (not summary['last_text_date'] or text_date > summary['last_text_date']):
                summary['last_text_date'] = text_date

        # Calculate response rate (inbound / outbound)
        if summary['outbound_texts'] > 0:
            summary['text_response_rate'] = round(summary['inbound_texts'] / summary['outbound_texts'] * 100)

        return summary

    def fetch_lead(self, person_id: int, verbose: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch all available data for a single lead.

        Args:
            person_id: The FUB person ID
            verbose: If True, print progress messages

        Returns:
            A dictionary containing all bundled data for the lead, or None if not found
        """
        if verbose:
            print(f"Fetching data for person {person_id}...")

        # Fetch person record
        person = self._fetch_person(person_id)
        if not person:
            if verbose:
                print(f"  Person {person_id} not found")
            return None

        # Fetch all related data
        notes = self._fetch_notes(person_id)
        calls = self._fetch_calls(person_id)
        events = self._fetch_events(person_id)
        tasks = self._fetch_tasks(person_id)
        emails = self._fetch_emails(person_id)
        text_messages = self._fetch_text_messages(person_id)

        if verbose:
            print(f"  Found: {len(notes)} notes, {len(calls)} calls, {len(text_messages)} texts, {len(events)} events, {len(tasks)} tasks, {len(emails)} emails")

        # Extract structured data
        market_values = self._extract_market_values(person)
        property_info = self._extract_property_info(person)
        summary = self._compute_summary(calls, notes, events, text_messages)

        # Sort all data chronologically (oldest first) for better narrative flow
        notes_sorted = sorted(notes, key=lambda x: x.get('created', ''))
        calls_sorted = sorted(calls, key=lambda x: x.get('created', ''))
        events_sorted = sorted(events, key=lambda x: x.get('created', ''))
        tasks_sorted = sorted(tasks, key=lambda x: x.get('created', ''))
        emails_sorted = sorted(emails, key=lambda x: x.get('created', ''))
        texts_sorted = sorted(text_messages, key=lambda x: x.get('created', ''))

        # Build the bundle
        bundle = {
            'person_id': person_id,
            'fetched_at': datetime.now().isoformat(),
            'fub_url': f"https://{FUB_SUBDOMAIN}.followupboss.com/2/people/view/{person_id}",

            # Basic lead info
            'lead_info': {
                'name': f"{person.get('firstName', '')} {person.get('lastName', '')}".strip(),
                'first_name': person.get('firstName'),
                'last_name': person.get('lastName'),
                'stage': person.get('stage'),
                'assigned_to': person.get('assignedTo'),
                'source': person.get('source'),
                'created': person.get('created'),
                'last_activity': person.get('lastActivity'),
                'tags': person.get('tags', []),
                'phones': [p.get('value') for p in person.get('phones', []) if p.get('value')],
                'emails': [e.get('value') for e in person.get('emails', []) if e.get('value')],
            },

            # Market values (for price comparison)
            'market_values': market_values,

            # Property information
            'property_info': property_info,

            # Raw data collections (sorted chronologically - oldest first)
            'notes': notes_sorted,
            'calls': calls_sorted,
            'text_messages': texts_sorted,  # Metadata only - content hidden by FUB
            'events': events_sorted,
            'tasks': tasks_sorted,
            'emails': emails_sorted,

            # Computed summary
            'summary': summary,

            # Full person record (for any other custom fields)
            '_raw_person': person,
        }

        return bundle

    def fetch_leads(self, person_ids: List[int], verbose: bool = False) -> List[Dict[str, Any]]:
        """
        Fetch all available data for multiple leads.

        Args:
            person_ids: List of FUB person IDs
            verbose: If True, print progress messages

        Returns:
            List of lead bundles
        """
        bundles = []
        total = len(person_ids)

        for i, person_id in enumerate(person_ids, 1):
            if verbose:
                print(f"[{i}/{total}] ", end="")

            bundle = self.fetch_lead(person_id, verbose=verbose)
            if bundle:
                bundles.append(bundle)

        return bundles

    def fetch_leads_by_stage(
        self,
        stage: str,
        limit: int = 100,
        verbose: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch all available data for leads in a specific stage.

        Args:
            stage: The stage name (e.g., "ACQ - Qualified")
            limit: Maximum number of leads to fetch
            verbose: If True, print progress messages

        Returns:
            List of lead bundles
        """
        if verbose:
            print(f"Fetching leads in stage: {stage}")

        # First, get the list of leads in this stage
        all_leads = []
        offset = 0

        while len(all_leads) < limit:
            result = self._make_request('people', {
                'stage': stage,
                'limit': min(100, limit - len(all_leads)),
                'offset': offset
            })

            if not result:
                break

            leads = result.get('people', [])
            all_leads.extend(leads)

            if len(leads) < 100:
                break

            offset += 100

        if verbose:
            print(f"Found {len(all_leads)} leads in stage '{stage}'")

        # Now fetch full data for each lead
        person_ids = [lead['id'] for lead in all_leads]
        return self.fetch_leads(person_ids, verbose=verbose)

    def fetch_leads_by_stages(
        self,
        stages: List[str],
        limit_per_stage: int = 100,
        verbose: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch all available data for leads across multiple stages.

        Args:
            stages: List of stage names
            limit_per_stage: Maximum number of leads per stage
            verbose: If True, print progress messages

        Returns:
            List of lead bundles
        """
        all_bundles = []

        for stage in stages:
            bundles = self.fetch_leads_by_stage(stage, limit_per_stage, verbose)
            all_bundles.extend(bundles)

        return all_bundles


def format_bundle_for_llm(bundle: Dict[str, Any], include_raw: bool = False) -> str:
    """
    Format a lead bundle as a text string suitable for LLM analysis.

    Args:
        bundle: The lead bundle from LeadDataFetcher
        include_raw: If True, include raw person record

    Returns:
        Formatted string containing all lead data
    """
    lines = []

    # Header
    lines.append("=" * 60)
    lines.append(f"LEAD DATA: {bundle['lead_info']['name']}")
    lines.append(f"Person ID: {bundle['person_id']}")
    lines.append(f"FUB Link: {bundle['fub_url']}")
    lines.append("=" * 60)

    # Basic info
    lines.append("\n## LEAD INFORMATION")
    info = bundle['lead_info']
    lines.append(f"Name: {info['name']}")
    lines.append(f"Stage: {info['stage']}")
    lines.append(f"Assigned To: {info['assigned_to']}")
    lines.append(f"Source: {info['source']}")
    lines.append(f"Created: {info['created']}")
    lines.append(f"Last Activity: {info['last_activity']}")
    if info['phones']:
        lines.append(f"Phone(s): {', '.join(info['phones'])}")
    if info['emails']:
        lines.append(f"Email(s): {', '.join(info['emails'])}")
    if info['tags']:
        lines.append(f"Tags: {', '.join(info['tags'])}")

    # Property info
    lines.append("\n## PROPERTY INFORMATION")
    prop = bundle['property_info']
    lines.append(f"County: {prop.get('county') or 'N/A'}")
    lines.append(f"State: {prop.get('state') or 'N/A'}")
    lines.append(f"Acreage: {prop.get('acreage') or 'N/A'}")
    lines.append(f"Road Frontage: {prop.get('road_frontage') or 'N/A'} ft")

    # Market values
    lines.append("\n## MARKET VALUES")
    mv = bundle['market_values']
    county_val = mv.get('county_value')
    li_val = mv.get('li_comp_value')
    lines.append(f"County Value (Market Total Parcel Value): {'$' + f'{county_val:,.0f}' if county_val else 'N/A'}")
    lines.append(f"Land Insights Comp Value (Market Value Estimate): {'$' + f'{li_val:,.0f}' if li_val else 'N/A'}")
    lines.append(f"LI Confidence: {prop.get('li_confidence') or 'N/A'}")

    # Summary stats
    lines.append("\n## ACTIVITY SUMMARY")
    summary = bundle['summary']
    lines.append(f"Total Notes: {summary['total_notes']}")
    lines.append(f"Total Calls: {summary['total_calls']} (Outbound: {summary['outbound_calls']}, Inbound: {summary['inbound_calls']})")
    lines.append(f"Connected Calls (60+ sec): {summary['connected_calls']}")
    lines.append(f"Total Talk Time: {summary['total_talk_time_seconds'] // 60} minutes")
    lines.append(f"Last Call: {summary['last_call_date'] or 'N/A'}")
    lines.append(f"Last Note: {summary['last_note_date'] or 'N/A'}")

    # Text message stats
    total_texts = summary.get('total_texts', 0)
    if total_texts > 0:
        outbound_texts = summary.get('outbound_texts', 0)
        inbound_texts = summary.get('inbound_texts', 0)
        response_rate = summary.get('text_response_rate')
        response_str = f" - {response_rate}% response rate" if response_rate is not None else ""
        lines.append(f"Total Texts: {total_texts} (Sent: {outbound_texts}, Received: {inbound_texts}{response_str})")
        lines.append(f"Last Text: {summary.get('last_text_date') or 'N/A'}")

    # Categorize notes by type
    agent_notes = []
    text_message_notes = []
    other_notes = []

    for note in bundle['notes']:
        subject = note.get('subject', '') or ''
        system_name = note.get('systemName', '')
        is_external = note.get('isExternal', False)

        # Check if this is a Smarter Contact text message history
        if 'smarter contact' in subject.lower() or (system_name == 'Zapier' and 'MESSAGING HISTORY' in note.get('body', '')):
            text_message_notes.append(note)
        # Manual agent notes (internal FUB notes)
        elif system_name == 'Follow Up Boss' and not is_external:
            agent_notes.append(note)
        else:
            other_notes.append(note)

    # Sort all note lists chronologically (oldest first) for better narrative flow
    agent_notes.sort(key=lambda x: x.get('created', ''))
    text_message_notes.sort(key=lambda x: x.get('created', ''))
    other_notes.sort(key=lambda x: x.get('created', ''))

    # Extract call notes from call records
    call_notes = []
    for call in bundle['calls']:
        if call.get('note'):
            call_notes.append({
                'date': call.get('created', 'N/A'),
                'agent': call.get('userName', 'Unknown'),
                'duration': call.get('duration', 0) or 0,
                'direction': 'Inbound' if call.get('isIncoming') else 'Outbound',
                'note': call.get('note')
            })

    # Sort call notes chronologically
    call_notes.sort(key=lambda x: x.get('date', ''))

    # Agent Notes section (manual notes by team members)
    if agent_notes:
        lines.append("\n## AGENT NOTES")
        lines.append("(Manual notes added by team members)")
        lines.append("-" * 40)
        for i, note in enumerate(agent_notes, 1):
            body = note.get('body', '')
            created = note.get('created', '')
            created_by = note.get('createdBy', '')

            lines.append(f"\n### Agent Note {i} - {created}")
            lines.append(f"By: {created_by}")
            if body:
                lines.append(f"Content:\n{body}")
            lines.append("-" * 40)

    # Call Notes section (notes attached to call records)
    if call_notes:
        lines.append("\n## CALL NOTES")
        lines.append("(Notes attached to call records after conversations)")
        lines.append("-" * 40)
        for i, cn in enumerate(call_notes, 1):
            lines.append(f"\n### Call Note {i} - {cn['date']}")
            lines.append(f"Agent: {cn['agent']} | {cn['direction']} call, {cn['duration']}s")
            lines.append(f"Notes:\n{cn['note']}")
            lines.append("-" * 40)

    # Text Message History section
    if text_message_notes:
        lines.append("\n## TEXT MESSAGE HISTORY")
        lines.append("(SMS conversations via Smarter Contact)")
        lines.append("-" * 40)
        for note in text_message_notes:
            body = note.get('body', '')
            created = note.get('created', '')
            lines.append(f"\n### Text Thread - Pushed {created}")
            lines.append(f"Content:\n{body}")
            lines.append("-" * 40)

    # Other notes (if any don't fit the categories above)
    if other_notes:
        lines.append("\n## OTHER NOTES")
        lines.append("-" * 40)
        for i, note in enumerate(other_notes, 1):
            subject = note.get('subject', '')
            body = note.get('body', '')
            created = note.get('created', '')
            created_by = note.get('createdBy', '')
            system = note.get('systemName', 'Unknown')

            lines.append(f"\n### Note {i} - {created}")
            lines.append(f"By: {created_by} | Source: {system}")
            if subject:
                lines.append(f"Subject: {subject}")
            if body:
                lines.append(f"Content:\n{body}")
            lines.append("-" * 40)

    # Call history (metadata only - notes extracted above)
    if bundle['calls']:
        lines.append("\n## CALL HISTORY")
        lines.append("(Call metadata - see CALL NOTES section for conversation details)")
        # Sort calls chronologically (oldest first)
        sorted_calls = sorted(bundle['calls'], key=lambda x: x.get('created', ''))
        for call in sorted_calls:
            date = call.get('created', 'N/A')
            duration = call.get('duration', 0) or 0
            direction = 'Inbound' if call.get('isIncoming') else 'Outbound'
            outcome = call.get('outcome') or 'N/A'
            user = call.get('userName', 'N/A')
            has_note = ' [HAS NOTES]' if call.get('note') else ''
            lines.append(f"- {date}: {direction}, {duration}s, Outcome: {outcome}, Agent: {user}{has_note}")

    # Events (sorted chronologically)
    if bundle['events']:
        lines.append("\n## EVENTS")
        sorted_events = sorted(bundle['events'], key=lambda x: x.get('created', ''))
        for event in sorted_events:
            date = event.get('created', 'N/A')
            event_type = event.get('type') or 'N/A'
            source = event.get('source') or 'N/A'
            message = event.get('message') or ''
            lines.append(f"- {date}: Type: {event_type}, Source: {source}")
            if message:
                lines.append(f"  Message: {message[:200]}")

    return "\n".join(lines)


# CLI for testing
if __name__ == '__main__':
    import json
    from lead_preprocessor import preprocess_bundle

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python lead_data_fetcher.py <person_id>           - Fetch single lead")
        print("  python lead_data_fetcher.py --stage 'ACQ - Qualified' [--limit 10]")
        print("")
        print("Options:")
        print("  --json     Output as JSON (preprocessed by default)")
        print("  --raw      Output raw JSON without preprocessing (for debugging)")
        print("  --verbose  Show progress messages")
        sys.exit(1)

    fetcher = LeadDataFetcher()
    output_json = '--json' in sys.argv
    output_raw = '--raw' in sys.argv
    verbose = '--verbose' in sys.argv

    if '--stage' in sys.argv:
        stage_idx = sys.argv.index('--stage')
        stage = sys.argv[stage_idx + 1]

        limit = 10
        if '--limit' in sys.argv:
            limit_idx = sys.argv.index('--limit')
            limit = int(sys.argv[limit_idx + 1])

        bundles = fetcher.fetch_leads_by_stage(stage, limit=limit, verbose=verbose)

        if output_json:
            for b in bundles:
                b.pop('_raw_person', None)
            if not output_raw:
                bundles = [preprocess_bundle(b) for b in bundles]
            print(json.dumps(bundles, indent=2, default=str))
        else:
            for bundle in bundles:
                print(format_bundle_for_llm(bundle))
                print("\n" + "=" * 80 + "\n")
    else:
        # Single person ID
        person_id = int(sys.argv[1])
        bundle = fetcher.fetch_lead(person_id, verbose=verbose)

        if bundle:
            if output_json:
                bundle.pop('_raw_person', None)
                if not output_raw:
                    bundle = preprocess_bundle(bundle)
                print(json.dumps(bundle, indent=2, default=str))
            else:
                print(format_bundle_for_llm(bundle))
        else:
            print(f"Lead {person_id} not found")
