#!/usr/bin/env python3
"""
Lead Data Preprocessor - Optimizes lead bundles before LLM submission

Reduces token count by 60-70% through:
- Compressing text message metadata to timeline format
- Splitting calls into unanswered attempts (grouped) and connected calls
- Stripping unnecessary metadata from notes
- Cleaning HTML and reformatting Smarter Contact message history
- Removing empty/null values throughout

Usage:
    from lead_preprocessor import preprocess_bundle

    raw_bundle = fetcher.fetch_lead(person_id)
    clean_bundle = preprocess_bundle(raw_bundle)
"""

import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict


def preprocess_bundle(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocess a lead bundle for LLM submission.

    Args:
        bundle: Raw bundle from LeadDataFetcher.fetch_lead()

    Returns:
        Cleaned and compressed bundle optimized for LLM analysis
    """
    if not bundle:
        return bundle

    result = {
        'person_id': bundle.get('person_id'),
        'fetched_at': bundle.get('fetched_at'),  # Keep full timestamp
    }

    # Process lead_info
    result['lead_info'] = _process_lead_info(bundle.get('lead_info', {}))

    # Keep market_values as-is (already clean)
    result['market_values'] = _remove_nulls(bundle.get('market_values', {}))

    # Process property_info (remove campaign_id and lead_pusher)
    result['property_info'] = _process_property_info(bundle.get('property_info', {}))

    # Process notes (strip metadata, clean HTML, reformat Smarter Contact)
    result['notes'] = _process_notes(bundle.get('notes', []))

    # Process calls (split into unanswered_attempts and connected_calls)
    result['calls'] = _process_calls(bundle.get('calls', []))

    # Process text_messages (compress to timeline)
    result['text_messages'] = _process_text_messages(bundle.get('text_messages', []))

    # Process tasks (light cleanup)
    result['tasks'] = _process_tasks(bundle.get('tasks', []))

    # Process summary - recalculate counts based on preprocessed data
    result['summary'] = _process_summary(
        bundle.get('summary', {}),
        processed_calls=result['calls'],
        processed_texts=result['text_messages'],
        processed_notes=result['notes']
    )

    # Remove events entirely (info captured in lead_info.created and source)
    # Don't include 'events' key at all

    # Remove emails if empty
    # Don't include 'emails' key at all (it's in lead_info if needed)

    # Remove any top-level empty values
    result = _remove_nulls(result)

    return result


def _truncate_to_date(timestamp: str) -> Optional[str]:
    """Convert ISO timestamp to date only (YYYY-MM-DD)"""
    if not timestamp:
        return None
    # Handle various timestamp formats
    if 'T' in timestamp:
        return timestamp.split('T')[0]
    return timestamp[:10] if len(timestamp) >= 10 else timestamp


def _remove_nulls(d: Dict) -> Dict:
    """Remove keys with null, empty string, or empty list values"""
    if not isinstance(d, dict):
        return d
    return {
        k: v for k, v in d.items()
        if v is not None and v != '' and v != [] and v != {}
    }


def _process_lead_info(info: Dict) -> Dict:
    """Process lead_info section"""
    if not info:
        return {}

    result = {
        'name': info.get('name'),
        'stage': info.get('stage'),
        'assigned_to': info.get('assigned_to'),
        'source': info.get('source'),
        'created': _truncate_to_date(info.get('created')),
        'last_activity': _truncate_to_date(info.get('last_activity')),
        'tags': info.get('tags', []),
        'phones': info.get('phones', []),
    }

    # Only include emails if non-empty
    emails = info.get('emails', [])
    if emails:
        result['emails'] = emails

    # Remove first_name/last_name (redundant with name)
    # Remove empty values
    return _remove_nulls(result)


def _process_property_info(info: Dict) -> Dict:
    """Process property_info section - remove campaign_id and lead_pusher"""
    if not info:
        return {}

    result = {
        'county': info.get('county'),
        'state': info.get('state'),
        'acreage': info.get('acreage'),
        'road_frontage': info.get('road_frontage'),
        'li_confidence': _to_int(info.get('li_confidence')),
    }

    # Explicitly exclude campaign_id and lead_pusher
    return _remove_nulls(result)


def _to_int(val) -> Optional[int]:
    """Convert value to int if possible"""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def _process_notes(notes: List[Dict]) -> List[Dict]:
    """Process notes - strip metadata, clean HTML, reformat Smarter Contact"""
    if not notes:
        return []

    processed = []
    for note in notes:
        clean_note = _process_single_note(note)
        if clean_note:
            processed.append(clean_note)

    return processed


def _process_single_note(note: Dict) -> Optional[Dict]:
    """Process a single note"""
    if not note:
        return None

    body = note.get('body', '')
    system_name = note.get('systemName', '')
    subject = note.get('subject', '')

    # Clean the body
    clean_body = _clean_note_body(body, system_name, subject)

    if not clean_body:
        return None

    result = {
        'date': _truncate_to_date(note.get('created')),
        'author': note.get('createdBy'),
        'body': clean_body,
    }

    # Include source only for non-FUB notes (Zapier, etc.)
    if system_name and system_name != 'Follow Up Boss':
        result['source'] = system_name

    # Include subject only if non-empty
    if subject:
        result['subject'] = subject

    # Check if updated by different person than created
    created_by = note.get('createdBy')
    updated_by = note.get('updatedBy')
    if updated_by and updated_by != created_by:
        result['edited_by'] = updated_by

    return _remove_nulls(result)


def _clean_note_body(body: str, system_name: str, subject: str) -> str:
    """Clean note body - strip HTML, reformat Smarter Contact messages"""
    if not body:
        return ''

    # Check if this is a Smarter Contact note
    is_smarter_contact = (
        'smarter contact' in subject.lower() or
        (system_name == 'Zapier' and 'MESSAGING HISTORY' in body)
    )

    if is_smarter_contact:
        return _reformat_smarter_contact(body)

    # Regular note - just clean HTML
    return _strip_html(body)


def _strip_html(text: str) -> str:
    """Strip HTML tags, convert mentions to @Name format"""
    if not text:
        return ''

    # Convert <span data-user-id="X">Name</span> to @Name
    text = re.sub(r'<span[^>]*data-user-id="[^"]*"[^>]*>([^<]+)</span>', r'@\1', text)

    # Convert <p>, </p>, <br>, <br/> to newlines
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>\s*<p>', '\n\n', text)
    text = re.sub(r'</?p>', '\n', text)

    # Strip all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    return text


def _reformat_smarter_contact(body: str) -> str:
    """Reformat Smarter Contact messaging history to compact format"""
    if not body:
        return ''

    # Remove LEAD INFORMATION block
    body = re.sub(
        r'LEAD INFORMATION\s*\n.*?(?=NOTES|MESSAGING HISTORY|$)',
        '',
        body,
        flags=re.DOTALL | re.IGNORECASE
    )

    # Remove empty NOTES section
    body = re.sub(r'NOTES\s*\n\s*\n', '', body)

    # Check if there's messaging history to parse
    if 'MESSAGING HISTORY' not in body:
        return body.strip()

    # Parse individual messages
    messages = []

    # Pattern to match message blocks
    # Each message has: contactPhone, content, date, direction, userPhone
    pattern = r'contactPhone:[^\n]*\ncontent:\s*(.+?)\ndate:\s*(\d{4}-\d{2}-\d{2})T[^\n]*\ndirection:\s*(sent|received)\nuserPhone:[^\n]*'

    for match in re.finditer(pattern, body, re.DOTALL):
        content = match.group(1).strip()
        date = match.group(2)
        direction = match.group(3).upper()

        messages.append(f"[{date} {direction}] {content}")

    if messages:
        return "MESSAGING HISTORY\n" + "\n".join(messages)

    # If parsing failed, return cleaned version
    # Remove repeated phone numbers at minimum
    body = re.sub(r'contactPhone:[^\n]*\n', '', body)
    body = re.sub(r'userPhone:[^\n]*\n', '', body)

    return body.strip()


def _process_calls(calls: List[Dict]) -> Dict:
    """
    Process calls - split into unanswered_attempts and connected_calls.

    Connected = duration > 180s (3 min) OR has note OR is inbound
    Calls under 180s are likely voicemails unless they have notes or are inbound.
    """
    if not calls:
        return {}

    unanswered = []
    connected = []

    CONNECTED_THRESHOLD_SECONDS = 180  # 3 minutes - shorter calls likely voicemails

    for call in calls:
        duration = call.get('duration', 0) or 0
        has_note = bool(call.get('note'))
        is_incoming = call.get('isIncoming', False)

        # Determine if connected (actual conversation vs voicemail/no answer)
        is_connected = duration > CONNECTED_THRESHOLD_SECONDS or has_note or is_incoming

        if is_connected:
            connected.append(_process_connected_call(call))
        else:
            unanswered.append(call)

    result = {}

    # Group and compress unanswered attempts
    if unanswered:
        result['unanswered_attempts'] = _compress_unanswered_calls(unanswered)

    # Keep connected calls as individual records
    if connected:
        result['connected_calls'] = connected

    return result


def _process_connected_call(call: Dict) -> Dict:
    """Process a single connected call"""
    result = {
        'date': _truncate_to_date(call.get('startedAt') or call.get('created')),
        'by': call.get('userName'),
        'duration_sec': call.get('duration', 0) or 0,
        'direction': 'inbound' if call.get('isIncoming') else 'outbound',
    }

    # Include note if present (CRITICAL - contains unique info)
    note = call.get('note')
    if note:
        result['note'] = note.strip()

    # Include outcome only if non-null
    outcome = call.get('outcome')
    if outcome:
        result['outcome'] = outcome

    return _remove_nulls(result)


def _compress_unanswered_calls(calls: List[Dict]) -> List[Dict]:
    """Compress unanswered calls - group by date and agent"""
    # Group by (date, agent)
    groups = defaultdict(int)

    for call in calls:
        date = _truncate_to_date(call.get('startedAt') or call.get('created'))
        agent = call.get('userName', 'Unknown')
        key = (date, agent)
        groups[key] += 1

    # Convert to list format
    result = []
    for (date, agent), count in sorted(groups.items()):
        entry = {'date': date, 'by': agent}
        if count > 1:
            entry['count'] = count
        result.append(entry)

    return result


def _process_text_messages(messages: List[Dict]) -> List[Dict]:
    """Compress text messages to timeline format"""
    if not messages:
        return []

    result = []
    for msg in messages:
        entry = {
            'date': _truncate_to_date(msg.get('sent') or msg.get('created')),
            'direction': 'inbound' if msg.get('isIncoming') else 'outbound',
        }

        # Only include 'by' for outbound messages
        if not msg.get('isIncoming'):
            entry['by'] = msg.get('userName')

        # Flag delivery failures
        delivery_status = msg.get('deliveryStatus')
        if delivery_status and delivery_status not in ('Delivered', None):
            entry['delivery_failed'] = True

        # Flag media attachments
        media = msg.get('media', [])
        if media:
            entry['has_media'] = True

        result.append(_remove_nulls(entry))

    return result


def _process_tasks(tasks: List[Dict]) -> List[Dict]:
    """Process tasks - light cleanup"""
    if not tasks:
        return []

    result = []
    for task in tasks:
        clean_task = {
            'name': task.get('name'),
            'due': task.get('dueDate'),
            'completed': bool(task.get('isCompleted')),
            'assigned_to': task.get('AssignedTo'),
            'last_updated': _truncate_to_date(task.get('updated')),
            'last_updated_by': task.get('updatedBy'),
        }
        result.append(_remove_nulls(clean_task))

    return result


def _process_summary(summary: Dict, processed_calls: Dict = None, processed_texts: List = None, processed_notes: List = None) -> Dict:
    """Process summary - recalculate counts based on preprocessed data"""
    if not summary:
        return {}

    # Calculate counts from preprocessed data
    connected_calls = processed_calls.get('connected_calls', []) if processed_calls else []
    unanswered_attempts = processed_calls.get('unanswered_attempts', []) if processed_calls else []
    texts = processed_texts or []
    notes = processed_notes or []

    # Count connected calls
    num_connected = len(connected_calls)

    # Count total unanswered (sum of counts, default 1 per entry)
    num_unanswered = sum(a.get('count', 1) for a in unanswered_attempts)

    # Total calls = connected + unanswered
    total_calls = num_connected + num_unanswered

    # Count inbound/outbound from connected calls
    inbound_calls = sum(1 for c in connected_calls if c.get('direction') == 'inbound')
    outbound_calls = sum(1 for c in connected_calls if c.get('direction') == 'outbound') + num_unanswered

    # Calculate talk time from connected calls
    total_talk_time = sum(c.get('duration_sec', 0) for c in connected_calls)

    # Count texts
    total_texts = len(texts)
    inbound_texts = sum(1 for t in texts if t.get('direction') == 'inbound')
    outbound_texts = sum(1 for t in texts if t.get('direction') == 'outbound')
    text_response_rate = round(inbound_texts / outbound_texts * 100) if outbound_texts > 0 else None

    result = {
        'total_notes': len(notes),
        'total_calls': total_calls,
        'total_talk_time_seconds': total_talk_time,
        'outbound_calls': outbound_calls,
        'inbound_calls': inbound_calls,
        'connected_calls': num_connected,
        'unanswered_attempts': num_unanswered,
        'last_call_date': _truncate_to_date(summary.get('last_call_date')),
        'last_note_date': _truncate_to_date(summary.get('last_note_date')),
        'total_texts': total_texts,
        'outbound_texts': outbound_texts,
        'inbound_texts': inbound_texts,
        'text_response_rate': text_response_rate,
        'last_text_date': _truncate_to_date(summary.get('last_text_date')),
    }

    return _remove_nulls(result)


# CLI for testing
if __name__ == '__main__':
    import sys
    import json
    from lead_data_fetcher import LeadDataFetcher

    if len(sys.argv) < 2:
        print("Usage: python lead_preprocessor.py <person_id>")
        print("       python lead_preprocessor.py <person_id> --compare  # Show before/after stats")
        sys.exit(1)

    person_id = int(sys.argv[1])
    compare_mode = '--compare' in sys.argv

    fetcher = LeadDataFetcher()
    raw_bundle = fetcher.fetch_lead(person_id, verbose=True)

    if not raw_bundle:
        print(f"Lead {person_id} not found")
        sys.exit(1)

    # Remove _raw_person for comparison (not sent to LLM anyway)
    raw_bundle.pop('_raw_person', None)

    # Preprocess
    clean_bundle = preprocess_bundle(raw_bundle)

    if compare_mode:
        raw_json = json.dumps(raw_bundle, indent=2, default=str)
        clean_json = json.dumps(clean_bundle, indent=2, default=str)

        raw_lines = len(raw_json.split('\n'))
        clean_lines = len(clean_json.split('\n'))

        print(f"\n{'='*50}")
        print(f"PREPROCESSING COMPARISON")
        print(f"{'='*50}")
        print(f"Raw JSON:   {raw_lines:,} lines / {len(raw_json):,} chars")
        print(f"Clean JSON: {clean_lines:,} lines / {len(clean_json):,} chars")
        print(f"Reduction:  {(1 - len(clean_json)/len(raw_json))*100:.1f}%")
        print(f"{'='*50}\n")

    # Output clean JSON
    print(json.dumps(clean_bundle, indent=2, default=str))
