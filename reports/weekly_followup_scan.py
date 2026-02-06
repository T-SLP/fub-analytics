#!/usr/bin/env python3
"""
Weekly Follow-up Lead Scanner

Scans leads in offer stages (ACQ - Needs Offer, ACQ - Offers Made, ACQ - Contract Sent)
for favorable pricing indicators from follow-up conversations with acquisition managers.

Only re-analyzes leads that have new notes/calls since the last scan.

Schedule: Run weekly (e.g., Monday 7:00 AM ET via GitHub Actions)

Usage:
    python weekly_followup_scan.py              # Normal run
    python weekly_followup_scan.py --dry-run    # No email, no LLM (uses mock data)
    python weekly_followup_scan.py --force      # Re-analyze all leads (ignore change detection)
"""

import os
import sys
import json
import hashlib
import smtplib
import ssl
from datetime import datetime, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# Load .env file from project root
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

# Add shared directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'shared'))

from database import get_db_client
from lead_data_fetcher import LeadDataFetcher, format_bundle_for_llm
from lead_preprocessor import preprocess_bundle

# Try to import Anthropic
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Configuration
FUB_SUBDOMAIN = os.getenv("FUB_SUBDOMAIN", "synergylandgroup")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_FROM = os.getenv("GMAIL_EMAIL")
EMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("HOT_SHEET_RECIPIENTS", "").split(",")

# Stages to scan for follow-up analysis
FOLLOWUP_STAGES = [
    # Active offer stages
    'ACQ - Needs Offer',
    'ACQ - Offers Made',
    'ACQ - Contract Sent',
    # Nurture stages (less frequent updates, but screen when activity occurs)
    'A - Lead (7 Days)',
    'B - Lead (15 Days)',
    'C - Lead (30 Days)',
    'D - Frozen (3 Months)',
]

# LLM Configuration
LLM_MODEL = "claude-sonnet-4-20250514"

# Default prompt file path
DEFAULT_PROMPT_FILE = Path(__file__).resolve().parent.parent / 'prompts' / 'weekly_followup_prompt.txt'

# Global to store current prompt file (can be overridden via --prompt argument)
CURRENT_PROMPT_FILE = DEFAULT_PROMPT_FILE
CURRENT_PROMPT_NAME = "default"

def load_system_prompt(prompt_file: Path = None):
    """Load the system prompt from external file"""
    file_to_load = prompt_file or CURRENT_PROMPT_FILE
    try:
        with open(file_to_load, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"[WARNING] Prompt file not found: {file_to_load}")
        print("[WARNING] Using default prompt")
        return """You are a real estate investment analyst. Analyze the lead and return JSON with:
{"sell_probability": "high/medium/low", "asking_price": number or null, "price_trend": "...", "key_indicators": "...", "red_flags": "...", "llm_notes": "..."}"""

def set_prompt_file(prompt_path: str):
    """Set the prompt file to use (for A/B testing different prompts)"""
    global CURRENT_PROMPT_FILE, CURRENT_PROMPT_NAME

    prompts_dir = Path(__file__).resolve().parent.parent / 'prompts'

    # If just a filename, look in prompts directory
    if not os.path.sep in prompt_path and not '/' in prompt_path:
        full_path = prompts_dir / prompt_path
    else:
        full_path = Path(prompt_path)

    if full_path.exists():
        CURRENT_PROMPT_FILE = full_path
        CURRENT_PROMPT_NAME = full_path.stem  # filename without extension
        print(f"[OK] Using prompt: {CURRENT_PROMPT_NAME} ({full_path})")
        return True
    else:
        print(f"[ERROR] Prompt file not found: {full_path}")
        return False


def get_leads_in_followup_stages(fetcher: LeadDataFetcher) -> List[Dict[str, Any]]:
    """Get all leads currently in follow-up stages"""
    all_bundles = []

    for stage in FOLLOWUP_STAGES:
        print(f"    Fetching leads in '{stage}'...")
        bundles = fetcher.fetch_leads_by_stage(stage, limit=200, verbose=False)
        print(f"    Found {len(bundles)} leads")
        all_bundles.extend(bundles)

    return all_bundles


def compute_data_hash(bundle: Dict[str, Any]) -> str:
    """Compute a hash of the lead data to detect changes"""
    # Include notes and calls in the hash
    hash_data = {
        'notes': [(n.get('id'), n.get('body', ''), n.get('updated')) for n in bundle.get('notes', [])],
        'calls': [(c.get('id'), c.get('duration'), c.get('created')) for c in bundle.get('calls', [])],
        'stage': bundle.get('lead_info', {}).get('stage'),
    }
    hash_str = json.dumps(hash_data, sort_keys=True, default=str)
    return hashlib.md5(hash_str.encode()).hexdigest()


def get_previous_analysis(db, person_id: str) -> Optional[Dict[str, Any]]:
    """Get the most recent analysis for a lead"""
    query = '''
        SELECT id, data_hash, sell_probability, asking_price, key_indicators,
               llm_notes, analyzed_at
        FROM lead_llm_analysis
        WHERE person_id = %s AND analysis_type = 'weekly_followup'
        ORDER BY analyzed_at DESC
        LIMIT 1
    '''

    results = db.execute_query(query, (person_id,))
    if results:
        row = results[0]
        return {
            'id': row[0],
            'data_hash': row[1],
            'sell_probability': row[2],
            'asking_price': row[3],
            'key_indicators': row[4],
            'llm_notes': row[5],
            'analyzed_at': row[6]
        }
    return None


def analyze_lead_with_llm(bundle: Dict[str, Any], client: Any,
                          previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Send lead data to Claude for analysis"""
    lead_context = format_bundle_for_llm(bundle)

    # Add context about previous analysis if available
    previous_context = ""
    if previous:
        previous_context = f"""

Previous Analysis (from {previous.get('analyzed_at', 'unknown date')}):
- Sell Probability: {previous.get('sell_probability', 'N/A')}
- Asking Price: ${previous.get('asking_price'):,.0f} if previous.get('asking_price') else 'Not stated'
- Key Indicators: {previous.get('key_indicators', 'N/A')}
- Notes: {previous.get('llm_notes', 'N/A')}

Compare the current data to the previous analysis and note any changes in the seller's position.
"""

    user_message = f"""Analyze this follow-up lead in the offer stage:

{lead_context}
{previous_context}
---

Assess if this seller is showing favorable indicators for closing a deal."""

    try:
        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=1000,
            # Use prompt caching for the system prompt (90% cost reduction on cached tokens)
            system=[
                {
                    "type": "text",
                    "text": load_system_prompt(),
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            messages=[
                {"role": "user", "content": user_message}
            ]
        )

        response_text = response.content[0].text

        # Parse JSON from response
        try:
            json_str = response_text
            if '```json' in response_text:
                json_str = response_text.split('```json')[1].split('```')[0]
            elif '```' in response_text:
                json_str = response_text.split('```')[1].split('```')[0]

            result = json.loads(json_str.strip())
            result['raw_response'] = response_text
            return result
        except json.JSONDecodeError:
            return {
                'sell_probability': 'unknown',
                'asking_price': None,
                'price_trend': 'unknown',
                'key_indicators': 'Parse error',
                'red_flags': 'Parse error',
                'llm_notes': response_text[:500],
                'raw_response': response_text
            }
    except Exception as e:
        return {
            'sell_probability': 'error',
            'asking_price': None,
            'price_trend': 'unknown',
            'key_indicators': 'LLM error',
            'red_flags': str(e),
            'llm_notes': str(e),
            'raw_response': None
        }


def mock_llm_analysis(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Mock LLM analysis for dry-run mode"""
    notes_text = ' '.join([n.get('body', '') for n in bundle.get('notes', [])])

    sell_probability = 'medium'
    if any(word in notes_text.lower() for word in ['motivated', 'flexible', 'negotiate', 'lower']):
        sell_probability = 'high'
    elif any(word in notes_text.lower() for word in ['firm', 'won\'t budge', 'realtor', 'other offer']):
        sell_probability = 'low'

    import re
    price_match = re.search(r'\$?([\d,]+(?:\.\d{2})?)', notes_text)
    asking_price = None
    if price_match:
        try:
            asking_price = float(price_match.group(1).replace(',', ''))
        except:
            pass

    return {
        'sell_probability': sell_probability,
        'asking_price': asking_price,
        'price_trend': 'unknown',
        'key_indicators': 'Mock analysis - dry run',
        'red_flags': 'None',
        'llm_notes': f'[DRY RUN] Would analyze {len(bundle.get("notes", []))} notes',
        'raw_response': None
    }


def store_analysis(db, person_id: str, person_name: str, stage: str,
                   data_hash: str, analysis: Dict[str, Any],
                   previous_id: Optional[int] = None, has_changed: bool = True,
                   prompt_version: str = None) -> int:
    """Store analysis results in database"""
    query = '''
        INSERT INTO lead_llm_analysis
        (person_id, person_name, stage, analysis_type, data_hash,
         sell_probability, key_indicators, asking_price, llm_notes,
         raw_llm_response, previous_analysis_id, has_changed, prompt_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    # Combine key_indicators and red_flags for storage
    indicators = analysis.get('key_indicators', '')
    red_flags = analysis.get('red_flags', '')
    if red_flags and red_flags != 'None':
        indicators = f"{indicators} | Red flags: {red_flags}"

    params = (
        person_id,
        person_name,
        stage,
        'weekly_followup',
        data_hash,
        analysis.get('sell_probability'),
        indicators,
        analysis.get('asking_price'),
        analysis.get('llm_notes'),
        json.dumps(analysis.get('raw_response')) if analysis.get('raw_response') else None,
        previous_id,
        has_changed,
        prompt_version or CURRENT_PROMPT_NAME
    )

    db.execute_update(query, params)
    return None


def record_run(db, leads_analyzed: int, leads_skipped: int,
               email_sent_to: List[str], status: str = 'completed', error: str = None):
    """Record the analysis run"""
    query = '''
        INSERT INTO lead_analysis_runs
        (run_type, leads_analyzed, leads_skipped, email_sent_to, run_status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    '''
    db.execute_update(query, ('weekly', leads_analyzed, leads_skipped,
                              email_sent_to, status, error))


def generate_email_html(results: List[Dict[str, Any]], skipped_count: int) -> str:
    """Generate HTML email report"""

    # Separate by stage
    by_stage = {}
    for result in results:
        stage = result.get('stage', 'Unknown')
        if stage not in by_stage:
            by_stage[stage] = []
        by_stage[stage].append(result)

    # Count by probability
    high_count = sum(1 for r in results if r['analysis'].get('sell_probability') == 'high')
    medium_count = sum(1 for r in results if r['analysis'].get('sell_probability') == 'medium')
    low_count = sum(1 for r in results if r['analysis'].get('sell_probability') == 'low')

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #1976d2; }}
            h2 {{ color: #424242; margin-top: 30px; border-bottom: 2px solid #1976d2; padding-bottom: 5px; }}
            .summary {{ background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
            th {{ background-color: #1976d2; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border: 1px solid #ddd; vertical-align: top; }}
            tr:nth-child(even) {{ background-color: #f5f5f5; }}
            .high {{ color: #2e7d32; font-weight: bold; }}
            .medium {{ color: #f57c00; font-weight: bold; }}
            .low {{ color: #c62828; font-weight: bold; }}
            .price {{ font-weight: bold; color: #1976d2; }}
            .trend-down {{ color: #2e7d32; }}
            .trend-up {{ color: #c62828; }}
            .notes {{ font-size: 0.9em; color: #666; }}
            .red-flag {{ color: #c62828; font-size: 0.85em; }}
            .changed {{ background-color: #fff3e0; }}
        </style>
    </head>
    <body>
        <h1>Weekly Follow-up Lead Analysis</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%A, %B %d, %Y at %I:%M %p ET')}</p>

        <div class="summary">
            <h3>Summary</h3>
            <p><strong>Leads with New Activity:</strong> {len(results)}</p>
            <p><strong>Leads Unchanged (skipped):</strong> {skipped_count}</p>
            <p>
                <span class="high">High Probability: {high_count}</span> |
                <span class="medium">Medium: {medium_count}</span> |
                <span class="low">Low: {low_count}</span>
            </p>
        </div>
    """

    if not results:
        html += "<p>No leads with new activity this week.</p>"
    else:
        # Process each stage
        # Use the same order as FOLLOWUP_STAGES
        stage_order = FOLLOWUP_STAGES

        for stage in stage_order:
            stage_results = by_stage.get(stage, [])
            if not stage_results:
                continue

            # Sort by probability (high first)
            priority_order = {'high': 0, 'medium': 1, 'low': 2, 'unknown': 3, 'error': 4}
            stage_results.sort(key=lambda x: priority_order.get(
                x['analysis'].get('sell_probability', 'unknown'), 99))

            html += f"""
            <h2>{stage} ({len(stage_results)} leads with updates)</h2>
            <table>
                <tr>
                    <th>Lead Name</th>
                    <th>Sell Probability</th>
                    <th>Asking Price</th>
                    <th>Price Trend</th>
                    <th>Key Indicators</th>
                    <th>Assessment</th>
                </tr>
            """

            for result in stage_results:
                analysis = result['analysis']
                prob = analysis.get('sell_probability', 'unknown')
                prob_class = prob if prob in ['high', 'medium', 'low'] else ''

                asking_price = analysis.get('asking_price')
                price_display = f"${asking_price:,.0f}" if asking_price else "Not stated"

                trend = analysis.get('price_trend', 'unknown')
                trend_class = 'trend-down' if trend == 'decreasing' else ('trend-up' if trend == 'increasing' else '')
                trend_display = trend.capitalize() if trend != 'unknown' else '-'

                name = result.get('person_name', 'Unknown')
                person_id = result.get('person_id')
                fub_url = f"https://{FUB_SUBDOMAIN}.followupboss.com/2/people/view/{person_id}"

                red_flags = analysis.get('red_flags', '')
                red_flag_html = f'<div class="red-flag">Flags: {red_flags}</div>' if red_flags and red_flags != 'None' else ''

                row_class = 'changed' if result.get('has_changed', True) else ''

                html += f"""
                <tr class="{row_class}">
                    <td><a href="{fub_url}" style="color: #1976d2;">{name}</a></td>
                    <td class="{prob_class}">{prob.upper()}</td>
                    <td class="price">{price_display}</td>
                    <td class="{trend_class}">{trend_display}</td>
                    <td>{analysis.get('key_indicators', 'N/A')}</td>
                    <td class="notes">{analysis.get('llm_notes', 'N/A')}{red_flag_html}</td>
                </tr>
                """

            html += "</table>"

    html += """
        <p style="margin-top: 30px; color: #666; font-size: 0.9em;">
            This report was generated automatically by the Lead Analysis System.<br>
            Only leads with new notes or calls since last week are included.
        </p>
    </body>
    </html>
    """

    return html


def send_email_report(html_content: str, recipient_list: List[str]) -> bool:
    """Send the email report"""
    if not all([EMAIL_FROM, EMAIL_PASSWORD, recipient_list]):
        print("[WARNING] Email configuration incomplete - skipping email")
        return False

    recipients = [r.strip() for r in recipient_list if r.strip()]
    if not recipients:
        print("[WARNING] No email recipients configured")
        return False

    try:
        message = MIMEMultipart('alternative')
        message['From'] = EMAIL_FROM
        message['To'] = ', '.join(recipients)
        message['Subject'] = f"Weekly Follow-up Lead Analysis - {date.today().strftime('%m/%d/%Y')}"

        html_part = MIMEText(html_content, 'html')
        message.attach(html_part)

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, recipients, message.as_string())

        print(f"[SUCCESS] Email sent to {len(recipients)} recipient(s)")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")
        return False


def main():
    """Main execution"""
    # Parse arguments
    dry_run = '--dry-run' in sys.argv
    force_all = '--force' in sys.argv

    # Handle custom prompt file for A/B testing
    if '--prompt' in sys.argv:
        prompt_idx = sys.argv.index('--prompt')
        if prompt_idx + 1 < len(sys.argv):
            if not set_prompt_file(sys.argv[prompt_idx + 1]):
                return 1  # Exit if prompt file not found

    print("=" * 60)
    print("WEEKLY FOLLOW-UP LEAD SCANNER")
    print(f"Date: {date.today()}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Force re-analyze all: {force_all}")
    print(f"Prompt: {CURRENT_PROMPT_NAME}")
    print("=" * 60)

    # Initialize clients
    db = get_db_client()
    fetcher = LeadDataFetcher()

    llm_client = None
    if not dry_run:
        if ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY:
            llm_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            print(f"[OK] Claude API initialized")
        else:
            print("[WARNING] Claude API not available - will use mock analysis")

    # Get leads in follow-up stages
    print(f"\n[1] Fetching leads in follow-up stages...")
    all_bundles = get_leads_in_followup_stages(fetcher)
    print(f"    Total leads: {len(all_bundles)}")

    if not all_bundles:
        print("\n[DONE] No leads in follow-up stages")
        record_run(db, 0, 0, [], 'completed')
        return 0

    # Preprocess all bundles first (reduces JSON size by ~80%)
    print(f"\n[2] Preprocessing {len(all_bundles)} leads...")
    preprocessed_bundles = []
    for bundle in all_bundles:
        preprocessed_bundles.append(preprocess_bundle(bundle))

    # Process each lead
    results = []
    skipped_count = 0

    print(f"\n[3] Analyzing leads (checking for changes)...")

    for i, bundle in enumerate(preprocessed_bundles, 1):
        person_id = str(bundle['person_id'])
        person_name = bundle['lead_info']['name']
        stage = bundle['lead_info']['stage']

        print(f"    [{i}/{len(preprocessed_bundles)}] {person_name}", end="")

        # Compute current data hash
        current_hash = compute_data_hash(bundle)

        # Check for previous analysis
        previous = get_previous_analysis(db, person_id)

        # Skip if unchanged (unless force mode)
        if previous and previous['data_hash'] == current_hash and not force_all:
            print(" - No changes, skipping")
            skipped_count += 1
            continue

        has_changed = previous is None or previous['data_hash'] != current_hash
        print(f" - {'NEW' if previous is None else 'CHANGED'}, analyzing...")

        # Analyze with LLM
        if dry_run or not llm_client:
            analysis = mock_llm_analysis(bundle)
        else:
            analysis = analyze_lead_with_llm(bundle, llm_client, previous)

        print(f"        Sell Probability: {analysis.get('sell_probability', 'unknown').upper()}")

        # Store in database (unless dry run)
        if not dry_run:
            previous_id = previous['id'] if previous else None
            store_analysis(db, person_id, person_name, stage,
                          current_hash, analysis, previous_id, has_changed)

        results.append({
            'person_id': person_id,
            'person_name': person_name,
            'stage': stage,
            'bundle': bundle,
            'analysis': analysis,
            'has_changed': has_changed
        })

    # Export combined preprocessed JSON (for testing or API batch processing)
    if dry_run and preprocessed_bundles:
        json_path = Path(__file__).parent.parent / 'test_exports' / 'weekly_followup_leads.json'
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(preprocessed_bundles, f, indent=2, default=str)
        print(f"    Exported {len(preprocessed_bundles)} preprocessed leads to: {json_path}")

    # Generate report
    print(f"\n[4] Generating email report...")
    html_report = generate_email_html(results, skipped_count)

    # Send email (unless dry run)
    if dry_run:
        print("    [DRY RUN] Skipping email send")
        preview_path = Path(__file__).parent.parent / 'weekly_followup_preview.html'
        with open(preview_path, 'w', encoding='utf-8') as f:
            f.write(html_report)
        print(f"    Preview saved to: {preview_path}")
    else:
        send_email_report(html_report, EMAIL_RECIPIENTS)

    # Record the run
    if not dry_run:
        record_run(db, len(results), skipped_count, EMAIL_RECIPIENTS, 'completed')

    # Summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Leads analyzed: {len(results)}")
    print(f"Leads skipped (no changes): {skipped_count}")
    if results:
        high = sum(1 for r in results if r['analysis'].get('sell_probability') == 'high')
        medium = sum(1 for r in results if r['analysis'].get('sell_probability') == 'medium')
        low = sum(1 for r in results if r['analysis'].get('sell_probability') == 'low')
        print(f"High probability: {high}")
        print(f"Medium probability: {medium}")
        print(f"Low probability: {low}")
    print("\n[DONE] Weekly scan complete!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
