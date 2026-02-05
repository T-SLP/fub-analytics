#!/usr/bin/env python3
"""
Daily Qualified Lead Scanner

Scans leads that moved into "ACQ - Qualified" stage today, analyzes their
cold caller notes using Claude API, and generates an email report assessing
each lead's sell probability.

Schedule: Run daily (e.g., 7:00 AM ET via GitHub Actions)

Usage:
    python daily_qualified_scan.py              # Normal run
    python daily_qualified_scan.py --dry-run    # No email, no LLM (uses mock data)
    python daily_qualified_scan.py --date 2026-01-28  # Scan specific date
"""

import os
import sys
import json
import hashlib
import smtplib
import ssl
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import List, Dict, Any, Optional

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

# LLM Configuration
LLM_MODEL = "claude-sonnet-4-20250514"

# Default prompt file path
DEFAULT_PROMPT_FILE = Path(__file__).resolve().parent.parent / 'prompts' / 'daily_qualified_prompt.txt'

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
{"sell_probability": "high/medium/low", "asking_price": number or null, "key_indicators": "...", "llm_notes": "..."}"""

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


def get_new_qualified_leads_today(db, target_date: Optional[date] = None) -> List[Dict[str, Any]]:
    """Get leads that moved into ACQ - Qualified on the target date"""
    if target_date is None:
        target_date = date.today()

    query = '''
        SELECT DISTINCT person_id, first_name, last_name, changed_at
        FROM stage_changes
        WHERE stage_to = 'ACQ - Qualified'
        AND DATE(changed_at) = %s
        ORDER BY changed_at DESC
    '''

    results = db.execute_query(query, (target_date,))

    return [
        {
            'person_id': str(row[0]),
            'person_name': f"{row[1] or ''} {row[2] or ''}".strip(),
            'changed_at': row[3]
        }
        for row in results
    ]


def compute_data_hash(bundle: Dict[str, Any]) -> str:
    """Compute a hash of the lead data to detect changes"""
    # Include notes and calls in the hash
    hash_data = {
        'notes': [(n.get('id'), n.get('body', '')) for n in bundle.get('notes', [])],
        'calls': [(c.get('id'), c.get('duration')) for c in bundle.get('calls', [])],
    }
    hash_str = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(hash_str.encode()).hexdigest()


def analyze_lead_with_llm(bundle: Dict[str, Any], client: Any) -> Dict[str, Any]:
    """Send lead data to Claude for analysis"""
    lead_context = format_bundle_for_llm(bundle)

    user_message = f"""Analyze this new qualified lead:

{lead_context}

---

Assess this lead's sell probability and extract pricing information."""

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
            # Handle potential markdown code blocks
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
                'key_indicators': 'Parse error',
                'llm_notes': response_text[:500],
                'raw_response': response_text
            }
    except Exception as e:
        return {
            'sell_probability': 'error',
            'asking_price': None,
            'key_indicators': 'LLM error',
            'llm_notes': str(e),
            'raw_response': None
        }


def mock_llm_analysis(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Mock LLM analysis for dry-run mode"""
    # Simple heuristic based on note content
    notes_text = ' '.join([n.get('body', '') for n in bundle.get('notes', [])])

    sell_probability = 'medium'
    if any(word in notes_text.lower() for word in ['motivated', 'quick', 'inherited', 'asap']):
        sell_probability = 'high'
    elif any(word in notes_text.lower() for word in ['firm', 'not negotiable', 'realtor']):
        sell_probability = 'low'

    # Try to find a price
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
        'key_indicators': 'Mock analysis - dry run',
        'llm_notes': f'[DRY RUN] Would analyze {len(bundle.get("notes", []))} notes',
        'raw_response': None
    }


def store_analysis(db, person_id: str, person_name: str, stage: str,
                   data_hash: str, analysis: Dict[str, Any], prompt_version: str = None) -> int:
    """Store analysis results in database"""
    query = '''
        INSERT INTO lead_llm_analysis
        (person_id, person_name, stage, analysis_type, data_hash,
         sell_probability, key_indicators, asking_price, llm_notes, raw_llm_response,
         prompt_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    params = (
        person_id,
        person_name,
        stage,
        'daily_qualified',
        data_hash,
        analysis.get('sell_probability'),
        analysis.get('key_indicators'),
        analysis.get('asking_price'),
        analysis.get('llm_notes'),
        json.dumps(analysis.get('raw_response')) if analysis.get('raw_response') else None,
        prompt_version or CURRENT_PROMPT_NAME
    )

    db.execute_update(query, params)
    return None


def record_run(db, run_type: str, leads_analyzed: int, leads_skipped: int,
               email_sent_to: List[str], status: str = 'completed', error: str = None):
    """Record the analysis run"""
    query = '''
        INSERT INTO lead_analysis_runs
        (run_type, leads_analyzed, leads_skipped, email_sent_to, run_status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    '''
    db.execute_update(query, (run_type, leads_analyzed, leads_skipped,
                              email_sent_to, status, error))


def generate_email_html(results: List[Dict[str, Any]], target_date: date) -> str:
    """Generate HTML email report"""

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
            .summary {{ background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 20px 0; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th {{ background-color: #1976d2; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border: 1px solid #ddd; vertical-align: top; }}
            tr:nth-child(even) {{ background-color: #f5f5f5; }}
            .high {{ color: #2e7d32; font-weight: bold; }}
            .medium {{ color: #f57c00; font-weight: bold; }}
            .low {{ color: #c62828; font-weight: bold; }}
            .price {{ font-weight: bold; color: #1976d2; }}
            .notes {{ font-size: 0.9em; color: #666; }}
        </style>
    </head>
    <body>
        <h1>Daily Qualified Lead Analysis</h1>
        <p><strong>Date:</strong> {target_date.strftime('%A, %B %d, %Y')}</p>
        <p><strong>Generated:</strong> {datetime.now().strftime('%I:%M %p ET')}</p>

        <div class="summary">
            <h3>Summary</h3>
            <p><strong>Total New Qualified Leads:</strong> {len(results)}</p>
            <p>
                <span class="high">High Probability: {high_count}</span> |
                <span class="medium">Medium: {medium_count}</span> |
                <span class="low">Low: {low_count}</span>
            </p>
        </div>
    """

    if not results:
        html += "<p>No new qualified leads today.</p>"
    else:
        # Sort by probability (high first)
        priority_order = {'high': 0, 'medium': 1, 'low': 2, 'unknown': 3, 'error': 4}
        results_sorted = sorted(results,
                               key=lambda x: priority_order.get(x['analysis'].get('sell_probability', 'unknown'), 99))

        html += """
        <table>
            <tr>
                <th>Lead Name</th>
                <th>Sell Probability</th>
                <th>Asking Price</th>
                <th>Key Indicators</th>
                <th>Notes</th>
            </tr>
        """

        for result in results_sorted:
            analysis = result['analysis']
            prob = analysis.get('sell_probability', 'unknown')
            prob_class = prob if prob in ['high', 'medium', 'low'] else ''

            asking_price = analysis.get('asking_price')
            price_display = f"${asking_price:,.0f}" if asking_price else "Not stated"

            name = result.get('person_name', 'Unknown')
            person_id = result.get('person_id')
            fub_url = f"https://{FUB_SUBDOMAIN}.followupboss.com/2/people/view/{person_id}"

            html += f"""
            <tr>
                <td><a href="{fub_url}" style="color: #1976d2;">{name}</a></td>
                <td class="{prob_class}">{prob.upper()}</td>
                <td class="price">{price_display}</td>
                <td>{analysis.get('key_indicators', 'N/A')}</td>
                <td class="notes">{analysis.get('llm_notes', 'N/A')}</td>
            </tr>
            """

        html += "</table>"

    html += """
        <p style="margin-top: 30px; color: #666; font-size: 0.9em;">
            This report was generated automatically by the Lead Analysis System.
        </p>
    </body>
    </html>
    """

    return html


def send_email_report(html_content: str, target_date: date, recipient_list: List[str]) -> bool:
    """Send the email report"""
    if not all([EMAIL_FROM, EMAIL_PASSWORD, recipient_list]):
        print("[WARNING] Email configuration incomplete - skipping email")
        return False

    # Filter empty recipients
    recipients = [r.strip() for r in recipient_list if r.strip()]
    if not recipients:
        print("[WARNING] No email recipients configured")
        return False

    try:
        message = MIMEMultipart('alternative')
        message['From'] = EMAIL_FROM
        message['To'] = ', '.join(recipients)
        message['Subject'] = f"Daily Qualified Lead Analysis - {target_date.strftime('%m/%d/%Y')}"

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

    target_date = date.today()
    if '--date' in sys.argv:
        date_idx = sys.argv.index('--date')
        if date_idx + 1 < len(sys.argv):
            target_date = datetime.strptime(sys.argv[date_idx + 1], '%Y-%m-%d').date()

    # Handle custom prompt file for A/B testing
    if '--prompt' in sys.argv:
        prompt_idx = sys.argv.index('--prompt')
        if prompt_idx + 1 < len(sys.argv):
            if not set_prompt_file(sys.argv[prompt_idx + 1]):
                return 1  # Exit if prompt file not found

    print("=" * 60)
    print("DAILY QUALIFIED LEAD SCANNER")
    print(f"Date: {target_date}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
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

    # Get new qualified leads
    print(f"\n[1] Fetching leads that entered ACQ - Qualified on {target_date}...")
    new_leads = get_new_qualified_leads_today(db, target_date)
    print(f"    Found {len(new_leads)} new qualified leads")

    if not new_leads:
        print("\n[DONE] No new qualified leads to analyze")
        record_run(db, 'daily', 0, 0, [], 'completed')
        return 0

    # Process each lead
    results = []
    print(f"\n[2] Analyzing {len(new_leads)} leads...")

    for i, lead in enumerate(new_leads, 1):
        person_id = lead['person_id']
        person_name = lead['person_name']
        print(f"    [{i}/{len(new_leads)}] {person_name} (ID: {person_id})")

        # Fetch full lead data
        bundle = fetcher.fetch_lead(int(person_id), verbose=False)
        if not bundle:
            print(f"        [SKIP] Could not fetch lead data")
            continue

        # Compute data hash
        data_hash = compute_data_hash(bundle)

        # Analyze with LLM
        if dry_run or not llm_client:
            analysis = mock_llm_analysis(bundle)
        else:
            analysis = analyze_lead_with_llm(bundle, llm_client)

        print(f"        Sell Probability: {analysis.get('sell_probability', 'unknown').upper()}")

        # Store in database (unless dry run)
        if not dry_run:
            store_analysis(db, person_id, person_name, 'ACQ - Qualified',
                          data_hash, analysis)

        results.append({
            'person_id': person_id,
            'person_name': person_name,
            'bundle': bundle,
            'analysis': analysis
        })

    # Generate report
    print(f"\n[3] Generating email report...")
    html_report = generate_email_html(results, target_date)

    # Send email (unless dry run)
    if dry_run:
        print("    [DRY RUN] Skipping email send")
        # Save HTML to file for preview
        preview_path = Path(__file__).parent.parent / 'daily_qualified_preview.html'
        with open(preview_path, 'w', encoding='utf-8') as f:
            f.write(html_report)
        print(f"    Preview saved to: {preview_path}")
    else:
        send_email_report(html_report, target_date, EMAIL_RECIPIENTS)

    # Record the run
    if not dry_run:
        record_run(db, 'daily', len(results), 0, EMAIL_RECIPIENTS, 'completed')

    # Summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Leads analyzed: {len(results)}")
    high = sum(1 for r in results if r['analysis'].get('sell_probability') == 'high')
    medium = sum(1 for r in results if r['analysis'].get('sell_probability') == 'medium')
    low = sum(1 for r in results if r['analysis'].get('sell_probability') == 'low')
    print(f"High probability: {high}")
    print(f"Medium probability: {medium}")
    print(f"Low probability: {low}")
    print("\n[DONE] Daily scan complete!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
