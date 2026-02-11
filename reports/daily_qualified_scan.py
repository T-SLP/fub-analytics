#!/usr/bin/env python3
"""
Daily Qualified Lead Triage

Fetches all leads from qualified stages, sends batch to Claude API for
ranking/scoring, and emails the triage report to Slack channel as a TXT attachment.

Schedule: 7:00 AM ET Monday-Friday via GitHub Actions

Usage:
    python daily_qualified_scan.py              # Normal run
    python daily_qualified_scan.py --dry-run    # No Slack, saves report locally
"""

import os
import sys
import json
import smtplib
import ssl
from datetime import datetime, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from typing import List, Dict, Any

# Load .env file from project root
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

# Add shared directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'shared'))

from lead_data_fetcher import LeadDataFetcher
from lead_preprocessor import preprocess_bundle

# Try to import Anthropic
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_FROM = os.getenv("GMAIL_EMAIL")
EMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
SLACK_CHANNEL_EMAIL = os.getenv("SLACK_CHANNEL_EMAIL",
    "closing-crew-aaaatdi5gbandco22uaz2ctcky@synergylandpa-awo4496.slack.com")

# LLM Configuration
LLM_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 16000  # Allow for detailed response with many leads

# Qualified stages to scan
QUALIFIED_STAGES = [
    'ACQ - Qualified',
    'Qualified Phase 2 - Day 3 to 2 Weeks',
    'Qualified Phase 3 - 2 Weeks to 4 Weeks',
]

# Prompt file path
PROMPT_FILE = Path(__file__).resolve().parent.parent / 'prompts' / 'daily_qualified_prompt.txt'


def load_system_prompt() -> str:
    """Load the system prompt from external file"""
    try:
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"[ERROR] Prompt file not found: {PROMPT_FILE}")
        sys.exit(1)


def fetch_qualified_leads(fetcher: LeadDataFetcher) -> List[Dict[str, Any]]:
    """Fetch all leads from the three qualified stages"""
    all_leads = []

    for stage in QUALIFIED_STAGES:
        print(f"  Fetching '{stage}'...")
        people = fetcher._make_request('people', {
            'stage': stage,
            'limit': 200
        })

        if people and 'people' in people:
            leads = people['people']
            print(f"    Found {len(leads)} leads")
            for lead in leads:
                lead['_stage'] = stage
            all_leads.extend(leads)
        else:
            print(f"    No leads found or API error")

    return all_leads


def fetch_and_preprocess_leads(fetcher: LeadDataFetcher, leads: List[Dict]) -> List[Dict[str, Any]]:
    """Fetch full data and preprocess each lead"""
    preprocessed = []

    for i, lead in enumerate(leads):
        person_id = lead['id']
        name = f"{lead.get('firstName', '')} {lead.get('lastName', '')}".strip()
        stage = lead.get('_stage', 'Unknown')
        print(f"    [{i+1}/{len(leads)}] {name} ({stage})...")

        try:
            bundle = fetcher.fetch_lead(person_id, verbose=False)
            if bundle:
                processed = preprocess_bundle(bundle)
                preprocessed.append(processed)
        except Exception as e:
            print(f"      Error: {e}")

    return preprocessed


def analyze_batch_with_llm(leads: List[Dict[str, Any]], client: Any) -> str:
    """Send all leads to Claude in a single batch for ranking"""

    # Format leads as JSON for the prompt
    leads_json = json.dumps(leads, indent=2, default=str)

    user_message = f"""Here are {len(leads)} qualified leads to triage and rank:

{leads_json}

Please analyze and rank these leads according to your scoring system."""

    print(f"  Sending {len(leads)} leads to Claude API...")

    try:
        response = client.messages.create(
            model=LLM_MODEL,
            max_tokens=MAX_TOKENS,
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
        print(f"  Received response ({len(response_text)} chars)")
        return response_text

    except Exception as e:
        print(f"  [ERROR] Claude API error: {e}")
        return f"ERROR: Failed to get response from Claude API.\n\nError details: {str(e)}"


def send_to_slack_via_email(report_text: str, lead_count: int) -> bool:
    """Send the triage report to Slack channel via email with TXT attachment"""
    if not all([EMAIL_FROM, EMAIL_PASSWORD, SLACK_CHANNEL_EMAIL]):
        print("[ERROR] Email configuration incomplete")
        if not EMAIL_FROM:
            print("       GMAIL_EMAIL not set")
        if not EMAIL_PASSWORD:
            print("       GMAIL_APP_PASSWORD not set")
        return False

    today = date.today().strftime('%Y-%m-%d')
    filename = f"qualified_lead_triage_{today}.txt"

    # Create email
    message = MIMEMultipart()
    message['From'] = EMAIL_FROM
    message['To'] = SLACK_CHANNEL_EMAIL
    message['Subject'] = f"Daily Qualified Lead Triage - {today}"

    # Email body (will show in Slack)
    body = f"""Daily Qualified Lead Triage Report

{lead_count} leads scored and ranked from all qualified stages (ACQ - Qualified, Phase 2, Phase 3).

Use this ranking to prioritize your calls today. Higher scores = stronger buy signals.

Full analysis attached as {filename}"""

    message.attach(MIMEText(body, 'plain'))

    # Attach the report as a TXT file
    attachment = MIMEBase('text', 'plain')
    attachment.set_payload(report_text.encode('utf-8'))
    encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition', f'attachment; filename="{filename}"')
    message.attach(attachment)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, SLACK_CHANNEL_EMAIL, message.as_string())

        print(f"  Sent report to Slack channel via email")
        return True

    except Exception as e:
        print(f"  [ERROR] Failed to send email: {e}")
        return False


def save_report_locally(report_text: str) -> Path:
    """Save the report to a local file"""
    today = date.today().strftime('%Y-%m-%d')
    filename = f"qualified_lead_triage_{today}.txt"

    output_dir = Path(__file__).resolve().parent.parent / 'test_exports'
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / filename
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report_text)

    return output_path


def mock_llm_response(leads: List[Dict[str, Any]]) -> str:
    """Generate a mock response for dry-run mode"""
    today = date.today().strftime('%Y-%m-%d')

    mock_response = f"""============================================
DAILY QUALIFIED LEAD TRIAGE
Batch: {len(leads)} leads analyzed
Date: {today}
============================================

RANKED SUMMARY
--------------------------------------------
Rank | Score | Conf | Stage | Lead Name              | Property
------------------------------------------------------------------------------------------------------------
"""

    for i, lead in enumerate(leads[:10], 1):  # Show first 10 in mock
        name = lead.get('lead_info', {}).get('name', 'Unknown')
        stage = lead.get('lead_info', {}).get('stage', 'Unknown')[:20]
        prop = lead.get('property_info', {})
        acreage = prop.get('acreage', '?')
        county = prop.get('county', '?')
        state = prop.get('state', '?')
        mock_response += f"{i}      {10-i+1}.0   M       {stage[:15]}  {name[:20]}  {acreage}ac {county}, {state}\n"

    mock_response += f"""
...and {len(leads) - 10} more leads

============================================
[DRY RUN - This is mock data, not actual LLM analysis]
============================================
"""
    return mock_response


def main():
    """Main execution"""
    dry_run = '--dry-run' in sys.argv

    print("=" * 60)
    print("DAILY QUALIFIED LEAD TRIAGE")
    print(f"Date: {date.today()}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 60)

    # Initialize
    fetcher = LeadDataFetcher()

    llm_client = None
    if not dry_run:
        if ANTHROPIC_AVAILABLE and ANTHROPIC_API_KEY:
            llm_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            print("[OK] Claude API initialized")
        else:
            print("[ERROR] Claude API not available")
            if not ANTHROPIC_AVAILABLE:
                print("       anthropic package not installed")
            if not ANTHROPIC_API_KEY:
                print("       ANTHROPIC_API_KEY not set")
            sys.exit(1)

    # Step 1: Fetch leads from qualified stages
    print(f"\n[1] Fetching leads from qualified stages...")
    stage_leads = fetch_qualified_leads(fetcher)
    print(f"    Total: {len(stage_leads)} leads")

    if not stage_leads:
        print("\n[DONE] No leads in qualified stages")
        return 0

    # Step 2: Fetch full data and preprocess
    print(f"\n[2] Fetching and preprocessing lead data...")
    preprocessed_leads = fetch_and_preprocess_leads(fetcher, stage_leads)
    print(f"    Preprocessed: {len(preprocessed_leads)} leads")

    # Step 3: Send to Claude for analysis
    print(f"\n[3] Analyzing leads with Claude API...")
    if dry_run:
        report_text = mock_llm_response(preprocessed_leads)
        print("  [DRY RUN] Using mock response")
    else:
        report_text = analyze_batch_with_llm(preprocessed_leads, llm_client)

    # Step 4: Save report locally
    print(f"\n[4] Saving report...")
    report_path = save_report_locally(report_text)
    print(f"    Saved to: {report_path}")

    # Step 5: Send to Slack via email
    if dry_run:
        print(f"\n[5] [DRY RUN] Skipping Slack delivery")
    else:
        print(f"\n[5] Sending to Slack channel...")
        send_to_slack_via_email(report_text, len(preprocessed_leads))

    # Summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Leads analyzed: {len(preprocessed_leads)}")
    print(f"Report saved: {report_path}")
    print("\n[DONE] Daily triage complete!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
