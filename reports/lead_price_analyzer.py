#!/usr/bin/env python3
"""
Lead Price Analyzer - Proof of Concept

Scans qualified leads in Follow Up Boss, extracts asking prices from notes,
and flags leads where the asking price is below market value.

This could indicate a motivated seller opportunity.
"""

import os
import sys
import re
import base64
import json
from datetime import datetime
from pathlib import Path

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

# Stages to scan for price analysis
QUALIFIED_STAGES = [
    'ACQ - Qualified',
    'Qualified Phase 2 - Day 3 to 2 Weeks',
    'Qualified Phase 3 - 2 Weeks to 4 Weeks',
    'ACQ - Needs Offer',
]


def get_auth_headers():
    """Get authentication headers for FUB API"""
    auth_string = base64.b64encode(f'{FUB_API_KEY}:'.encode()).decode()
    return {
        'Authorization': f'Basic {auth_string}',
        'Content-Type': 'application/json'
    }


def extract_price_from_text(text):
    """
    Extract price/dollar amounts from text using regex.
    Returns a list of (amount, context) tuples.
    """
    prices = []

    # Pattern 1: Price Objective field (most reliable)
    price_obj_match = re.search(
        r'Price\s*Objective[:\s]*\$?([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?',
        text,
        re.IGNORECASE
    )
    if price_obj_match:
        amount = parse_price_amount(price_obj_match.group(1), price_obj_match.group(0))
        if amount:
            prices.append((amount, 'Price Objective field'))

    # Pattern 2: Asking price variations
    asking_patterns = [
        r'asking\s*(?:price)?[:\s]*\$?([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?',
        r'wants?\s*\$?([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?',
        r'looking\s*for\s*\$?([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?',
        r'\$\s*([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?\s*(?:asking|for the|for it)',
    ]

    for pattern in asking_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount = parse_price_amount(match.group(1), match.group(0))
            if amount and (amount, 'Price Objective field') not in prices:
                prices.append((amount, f'Pattern: {pattern[:30]}...'))

    # Pattern 3: Dollar amounts in conversation context
    # "$39,000 we can talk" or similar
    dollar_matches = re.findall(r'\$([\d,]+(?:\.\d{2})?)\s*(?:k|K|thousand|million|M)?', text)
    for match in dollar_matches:
        amount = parse_price_amount(match, text)
        if amount and not any(p[0] == amount for p in prices):
            prices.append((amount, 'Dollar amount in text'))

    return prices


def parse_price_amount(amount_str, context=''):
    """Parse a price string into a numeric value"""
    try:
        # Remove commas
        amount_str = amount_str.replace(',', '')
        amount = float(amount_str)

        # Check for multipliers (k, K, thousand, million, M)
        context_lower = context.lower()
        if 'million' in context_lower or 'm' in context_lower.split()[-1:]:
            # Be careful - only multiply by million if explicitly stated
            if 'million' in context_lower or (amount < 100 and re.search(r'\d\s*M\b', context, re.IGNORECASE)):
                amount *= 1_000_000
        elif 'k' in context_lower or 'thousand' in context_lower:
            if amount < 10000:  # Only apply K multiplier for smaller numbers
                amount *= 1000

        # Sanity check - land prices typically between $1k and $50M
        if 1000 <= amount <= 50_000_000:
            return amount
        elif amount < 1000 and amount > 0:
            # Might be per-acre price, still return it
            return amount

        return None
    except (ValueError, TypeError):
        return None


def extract_acreage_from_text(text):
    """Extract acreage from note text"""
    patterns = [
        r'(?:Confirm\s*)?[Aa]creage[s]?[:\s]*(\d+(?:\.\d+)?)',
        r'(\d+(?:\.\d+)?)\s*acres?',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
    return None


def fetch_leads_by_stage(stage, limit=100):
    """Fetch leads from a specific stage"""
    headers = get_auth_headers()
    all_leads = []
    offset = 0

    while True:
        response = requests.get(
            'https://api.followupboss.com/v1/people',
            params={
                'stage': stage,
                'limit': min(limit - len(all_leads), 100),
                'offset': offset
            },
            headers=headers,
            timeout=30
        )

        if response.status_code != 200:
            print(f"  Error fetching leads: {response.status_code}")
            break

        data = response.json()
        leads = data.get('people', [])
        all_leads.extend(leads)

        if len(leads) < 100 or len(all_leads) >= limit:
            break

        offset += 100

    return all_leads


def fetch_notes_for_person(person_id):
    """Fetch all notes for a person"""
    headers = get_auth_headers()

    response = requests.get(
        'https://api.followupboss.com/v1/notes',
        params={'personId': person_id, 'limit': 100},
        headers=headers,
        timeout=30
    )

    if response.status_code == 200:
        return response.json().get('notes', [])
    return []


def analyze_lead(lead):
    """
    Analyze a lead for price vs market value comparison.
    Returns analysis dict or None if no price found.
    """
    person_id = lead.get('id')
    name = f"{lead.get('firstName', '')} {lead.get('lastName', '')}".strip()
    stage = lead.get('stage', '')

    # Get market values from custom fields
    county_value = lead.get('customMarketTotalParcelValue')
    li_comp_value = lead.get('customMarketValueEstimate')
    acreage = lead.get('customAcreage')
    county = lead.get('customParcelCounty', '')
    state = lead.get('customParcelState', '')

    # Parse market values
    market_value = None
    market_value_source = None

    if county_value:
        try:
            market_value = float(county_value)
            market_value_source = 'County Value'
        except (ValueError, TypeError):
            pass

    if not market_value and li_comp_value:
        try:
            market_value = float(li_comp_value)
            market_value_source = 'LI Comp Value'
        except (ValueError, TypeError):
            pass

    # Fetch and analyze notes
    notes = fetch_notes_for_person(person_id)

    asking_price = None
    asking_price_source = None
    note_acreage = None
    relevant_note_text = None

    for note in notes:
        body = note.get('body', '') or ''
        subject = note.get('subject', '') or ''
        full_text = f"{subject}\n{body}"

        # Extract prices
        prices = extract_price_from_text(full_text)
        if prices:
            # Take the first (most reliable) price found
            asking_price, asking_price_source = prices[0]
            relevant_note_text = body[:500] if body else subject[:500]

        # Extract acreage if not in custom fields
        if not acreage:
            note_acreage = extract_acreage_from_text(full_text)
            if note_acreage:
                acreage = note_acreage

        if asking_price:
            break  # Found what we need

    if not asking_price:
        return None  # No price found in notes

    # Calculate per-acre prices if acreage available
    asking_per_acre = None
    market_per_acre = None

    if acreage and acreage > 0:
        try:
            acreage = float(acreage)
            asking_per_acre = asking_price / acreage
            if market_value:
                market_per_acre = market_value / acreage
        except (ValueError, TypeError, ZeroDivisionError):
            pass

    # Determine if this is a potential opportunity (asking < market)
    is_below_market = False
    discount_percent = None

    if market_value and asking_price:
        if asking_price < market_value:
            is_below_market = True
            discount_percent = ((market_value - asking_price) / market_value) * 100

    return {
        'person_id': person_id,
        'name': name,
        'stage': stage,
        'county': county,
        'state': state,
        'acreage': acreage,
        'asking_price': asking_price,
        'asking_price_source': asking_price_source,
        'asking_per_acre': asking_per_acre,
        'market_value': market_value,
        'market_value_source': market_value_source,
        'market_per_acre': market_per_acre,
        'is_below_market': is_below_market,
        'discount_percent': discount_percent,
        'note_excerpt': relevant_note_text,
        'fub_url': f"https://{FUB_SUBDOMAIN}.followupboss.com/2/people/view/{person_id}"
    }


def format_currency(amount):
    """Format a number as currency"""
    if amount is None:
        return 'N/A'
    if amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    elif amount >= 1000:
        return f"${amount:,.0f}"
    else:
        return f"${amount:.2f}"


def main():
    """Main execution"""
    print("=" * 80)
    print("LEAD PRICE ANALYZER - Proof of Concept")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    if not FUB_API_KEY:
        print("[ERROR] FUB_API_KEY not set")
        return 1

    all_analyzed = []
    below_market = []

    # Process each qualified stage
    for stage in QUALIFIED_STAGES:
        print(f"\n[SCANNING] {stage}...")
        leads = fetch_leads_by_stage(stage, limit=50)  # Limit for POC
        print(f"  Found {len(leads)} leads")

        for lead in leads:
            analysis = analyze_lead(lead)
            if analysis:
                all_analyzed.append(analysis)
                if analysis['is_below_market']:
                    below_market.append(analysis)

    # Report results
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nTotal leads scanned: {sum(len(fetch_leads_by_stage(s, limit=50)) for s in QUALIFIED_STAGES)}")
    print(f"Leads with asking price found: {len(all_analyzed)}")
    print(f"Leads with asking BELOW market value: {len(below_market)}")

    # Show all leads with prices
    if all_analyzed:
        print("\n" + "-" * 80)
        print("ALL LEADS WITH ASKING PRICES")
        print("-" * 80)

        for lead in all_analyzed:
            flag = " *** BELOW MARKET ***" if lead['is_below_market'] else ""
            discount = f" ({lead['discount_percent']:.1f}% below)" if lead['discount_percent'] else ""

            print(f"""
Name: {lead['name']}{flag}
Stage: {lead['stage']}
Location: {lead['county']}, {lead['state']}
Acreage: {lead['acreage'] or 'N/A'}
Asking Price: {format_currency(lead['asking_price'])} (from: {lead['asking_price_source']})
Market Value: {format_currency(lead['market_value'])}{discount} (source: {lead['market_value_source'] or 'N/A'})
Per Acre - Asking: {format_currency(lead['asking_per_acre'])} | Market: {format_currency(lead['market_per_acre'])}
FUB Link: {lead['fub_url']}
""")

    # Highlight opportunities
    if below_market:
        print("\n" + "=" * 80)
        print("🎯 POTENTIAL OPPORTUNITIES - ASKING BELOW MARKET VALUE")
        print("=" * 80)

        # Sort by discount percentage
        below_market.sort(key=lambda x: x['discount_percent'] or 0, reverse=True)

        for lead in below_market:
            print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{lead['name']} - {lead['discount_percent']:.1f}% BELOW MARKET
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Location: {lead['county']}, {lead['state']}
Acreage: {lead['acreage'] or 'N/A'}
Asking: {format_currency(lead['asking_price'])}
Market: {format_currency(lead['market_value'])}
Potential Savings: {format_currency(lead['market_value'] - lead['asking_price']) if lead['market_value'] else 'N/A'}

Note Excerpt:
{lead['note_excerpt'][:300] if lead['note_excerpt'] else 'N/A'}...

Link: {lead['fub_url']}
""")

    print("\n[DONE] Analysis complete!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
