#!/usr/bin/env python3
"""
Export all leads from qualified stages as a single combined JSON file.

Includes leads from all three qualified stages (pre-contact):
- ACQ - Qualified
- Qualified Phase 2 - Day 3 to 2 Weeks
- Qualified Phase 3 - 2 Weeks to 4 Weeks
"""

import os
import sys
import json

# Add shared directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'shared'))

from lead_data_fetcher import LeadDataFetcher
from lead_preprocessor import preprocess_bundle

# All qualified stages (leads awaiting initial contact)
QUALIFIED_STAGES = [
    'ACQ - Qualified',
    'Qualified Phase 2 - Day 3 to 2 Weeks',
    'Qualified Phase 3 - 2 Weeks to 4 Weeks',
]


def main():
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), 'test_exports')
    os.makedirs(output_dir, exist_ok=True)

    fetcher = LeadDataFetcher()

    # Fetch leads from all qualified stages
    all_stage_leads = []
    for stage in QUALIFIED_STAGES:
        print(f"Fetching leads in '{stage}'...")
        people = fetcher._make_request('people', {
            'stage': stage,
            'limit': 200
        })

        if people and 'people' in people:
            leads = people['people']
            print(f"  Found {len(leads)} leads")
            for lead in leads:
                lead['_stage'] = stage  # Track which stage they came from
            all_stage_leads.extend(leads)
        else:
            print(f"  No leads found or API error")

    print(f"\nTotal leads across all qualified stages: {len(all_stage_leads)}")

    if not all_stage_leads:
        print("No leads to export")
        return

    # Collect all preprocessed leads
    all_leads = []
    for i, lead in enumerate(all_stage_leads):
        person_id = lead['id']
        name = f"{lead.get('firstName', '')} {lead.get('lastName', '')}".strip()
        stage = lead.get('_stage', 'Unknown')
        print(f"  [{i+1}/{len(all_stage_leads)}] Fetching {person_id}: {name} ({stage})...")

        try:
            bundle = fetcher.fetch_lead(person_id, verbose=False)
            if bundle:
                # Preprocess the bundle
                processed = preprocess_bundle(bundle)
                all_leads.append(processed)
        except Exception as e:
            print(f"    Error: {e}")

    # Write combined JSON file
    output_path = os.path.join(output_dir, 'qualified_leads.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_leads, f, indent=2, default=str)

    print(f"\nExported {len(all_leads)} leads to {output_path}")


if __name__ == '__main__':
    main()
