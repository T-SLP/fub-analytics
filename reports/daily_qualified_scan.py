#!/usr/bin/env python3
"""
Daily Qualified Lead Triage

Fetches all leads from qualified stages, sends batch to Claude API for
ranking/scoring, uploads report to Google Drive, and posts link to Slack.

Schedule: 6:00 AM ET Monday-Friday via GitHub Actions

Usage:
    python daily_qualified_scan.py              # Normal run
    python daily_qualified_scan.py --dry-run    # No Slack, no upload, saves report locally
"""

import os
import sys
import json
import requests
from datetime import date
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

from lead_data_fetcher import LeadDataFetcher
from lead_preprocessor import preprocess_bundle

# Try to import Anthropic
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# Try to import Google Drive libraries
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False

# Configuration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
GOOGLE_DRIVE_CREDENTIALS = os.getenv("GOOGLE_DRIVE_CREDENTIALS")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

# LLM Configuration
LLM_MODEL = "claude-opus-4-6"
MAX_TOKENS = 32000  # Total budget for thinking + visible output
THINKING_BUDGET = 16000  # Extended thinking budget for internal scoring/ranking

# Qualified stages to scan
QUALIFIED_STAGES = [
    'ACQ - Qualified',
    'Qualified Phase 2 - Day 3 to 2 Weeks',
    'Qualified Phase 3 - 2 Weeks to 4 Weeks',
]

# Stage abbreviations for report output (locked by Python, not LLM-interpreted)
STAGE_ABBREVIATIONS = {
    'ACQ - Qualified': 'ACQ-Q',
    'Qualified Phase 2 - Day 3 to 2 Weeks': 'Ph2',
    'Qualified Phase 3 - 2 Weeks to 4 Weeks': 'Ph3',
}

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
                # Inject stage abbreviation from Python (not LLM-interpreted)
                actual_stage = processed.get('lead_info', {}).get('stage', '')
                processed.setdefault('lead_info', {})['stage_abbrev'] = STAGE_ABBREVIATIONS.get(actual_stage, actual_stage)
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
        # Use streaming to avoid 10-minute timeout on long-running requests
        response_text = ""
        with client.messages.stream(
            model=LLM_MODEL,
            max_tokens=MAX_TOKENS,
            thinking={
                "type": "enabled",
                "budget_tokens": THINKING_BUDGET
            },
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
        ) as stream:
            for event in stream:
                # Collect only visible text events (skip thinking)
                if event.type == "content_block_start" and hasattr(event.content_block, 'text'):
                    response_text += event.content_block.text
                elif event.type == "content_block_delta" and hasattr(event.delta, 'text'):
                    response_text += event.delta.text

        print(f"  Received response ({len(response_text)} chars)")
        return response_text

    except Exception as e:
        print(f"  [ERROR] Claude API error: {e}")
        return f"ERROR: Failed to get response from Claude API.\n\nError details: {str(e)}"


def upload_to_google_drive(file_path: Path) -> Optional[str]:
    """Upload file to Google Drive and return the view link"""
    if not GOOGLE_DRIVE_AVAILABLE:
        print("  [ERROR] Google Drive libraries not installed")
        return None

    if not GOOGLE_DRIVE_CREDENTIALS or not GOOGLE_DRIVE_FOLDER_ID:
        print("  [ERROR] Google Drive credentials not configured")
        return None

    try:
        # Parse credentials
        creds_dict = json.loads(GOOGLE_DRIVE_CREDENTIALS)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=credentials)

        # Upload file
        file_metadata = {
            'name': file_path.name,
            'parents': [GOOGLE_DRIVE_FOLDER_ID]
        }
        media = MediaFileUpload(str(file_path), mimetype='text/plain', resumable=True)

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink',
            supportsAllDrives=True
        ).execute()

        link = file.get('webViewLink')
        print(f"  Uploaded to Google Drive: {file.get('name')}")
        print(f"  Link: {link}")
        return link

    except Exception as e:
        print(f"  [ERROR] Google Drive upload failed: {e}")
        return None


def post_to_slack(lead_count: int, drive_link: str) -> bool:
    """Post triage summary to Slack with Google Drive link"""
    if not SLACK_WEBHOOK_URL:
        print("  [ERROR] SLACK_WEBHOOK_URL not configured")
        return False

    today = date.today().strftime('%Y-%m-%d')

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Daily Qualified Lead Triage - {today}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{lead_count} leads* analyzed and ranked from all qualified stages:\n• ACQ - Qualified\n• Qualified Phase 2\n• Qualified Phase 3"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Use this ranking to prioritize your calls today. Higher scores = stronger buy signals."
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Full Report",
                            "emoji": True
                        },
                        "url": drive_link,
                        "style": "primary"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=message,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        print(f"  Posted to Slack successfully")
        return True

    except Exception as e:
        print(f"  [ERROR] Slack post failed: {e}")
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

    # Step 5: Upload to Google Drive
    drive_link = None
    if dry_run:
        print(f"\n[5] [DRY RUN] Skipping Google Drive upload")
    else:
        print(f"\n[5] Uploading to Google Drive...")
        drive_link = upload_to_google_drive(report_path)

    # Step 6: Post to Slack
    if dry_run:
        print(f"\n[6] [DRY RUN] Skipping Slack post")
    elif drive_link:
        print(f"\n[6] Posting to Slack...")
        post_to_slack(len(preprocessed_leads), drive_link)
    else:
        print(f"\n[6] Skipping Slack post (no Drive link available)")

    # Summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Leads analyzed: {len(preprocessed_leads)}")
    print(f"Report saved: {report_path}")
    if drive_link:
        print(f"Google Drive: {drive_link}")
    print("\n[DONE] Daily triage complete!")

    return 0


if __name__ == '__main__':
    sys.exit(main())
