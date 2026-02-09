#!/usr/bin/env python3
"""
Export leads from Follow Up Boss by stage group.

Usage:
    python export_leads.py --group qualified      # Pre-contact qualified leads
    python export_leads.py --group lead-management  # ABCD nurture stages
    python export_leads.py --group lead-management --stages C,D  # Just C and D
    python export_leads.py --group pipeline       # Active deal stages

All exports produce a single combined JSON file with preprocessed lead data.
"""

import os
import sys
import json
import argparse

# Add shared directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'shared'))

from lead_data_fetcher import LeadDataFetcher
from lead_preprocessor import preprocess_bundle

# Stage group definitions with aliases for filtering
STAGE_GROUPS = {
    'qualified': {
        'description': 'Pre-contact qualified leads awaiting initial contact',
        'stages': [
            'ACQ - Qualified',
            'Qualified Phase 2 - Day 3 to 2 Weeks',
            'Qualified Phase 3 - 2 Weeks to 4 Weeks',
        ],
        'aliases': {
            '1': 'ACQ - Qualified',
            'phase1': 'ACQ - Qualified',
            '2': 'Qualified Phase 2 - Day 3 to 2 Weeks',
            'phase2': 'Qualified Phase 2 - Day 3 to 2 Weeks',
            '3': 'Qualified Phase 3 - 2 Weeks to 4 Weeks',
            'phase3': 'Qualified Phase 3 - 2 Weeks to 4 Weeks',
        },
        'output_file': 'qualified_leads.json',
    },
    'lead-management': {
        'description': 'ABCD lead nurture stages',
        'stages': [
            'A - Lead (7 Days)',
            'B - Lead (15 Days)',
            'C - Lead (30 Days)',
            'D - Frozen (3 Months)',
        ],
        'aliases': {
            'A': 'A - Lead (7 Days)',
            'B': 'B - Lead (15 Days)',
            'C': 'C - Lead (30 Days)',
            'D': 'D - Frozen (3 Months)',
        },
        'output_file': 'lead_management_leads.json',
    },
    'pipeline': {
        'description': 'Active acquisition pipeline (offers and contracts)',
        'stages': [
            'ACQ - Needs Offer',
            'ACQ - Contract Sent',
            'ACQ - Under Contract',
        ],
        'aliases': {
            'needs-offer': 'ACQ - Needs Offer',
            'contract-sent': 'ACQ - Contract Sent',
            'under-contract': 'ACQ - Under Contract',
        },
        'output_file': 'pipeline_leads.json',
    },
}


def resolve_stages(group_name: str, stage_filter: str = None) -> list:
    """
    Resolve stage names from filter string, supporting aliases.

    Args:
        group_name: The stage group name
        stage_filter: Comma-separated list of stage names or aliases (e.g., "C,D" or "phase2,phase3")

    Returns:
        List of full stage names
    """
    group = STAGE_GROUPS[group_name]
    all_stages = group['stages']
    aliases = group.get('aliases', {})

    if not stage_filter:
        return all_stages

    # Parse comma-separated filter
    requested = [s.strip() for s in stage_filter.split(',')]

    resolved = []
    for req in requested:
        # Check if it's an alias (case-insensitive)
        req_lower = req.lower()
        req_upper = req.upper()

        if req_upper in aliases:
            resolved.append(aliases[req_upper])
        elif req_lower in aliases:
            resolved.append(aliases[req_lower])
        elif req in aliases:
            resolved.append(aliases[req])
        elif req in all_stages:
            # Exact match to full stage name
            resolved.append(req)
        else:
            # Try partial match
            matches = [s for s in all_stages if req.lower() in s.lower()]
            if matches:
                resolved.extend(matches)
            else:
                print(f"Warning: Unknown stage '{req}' - skipping")

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for s in resolved:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    return unique


def export_leads(group_name: str, stage_filter: str = None) -> None:
    """Export leads for a specific stage group."""
    if group_name not in STAGE_GROUPS:
        print(f"Error: Unknown group '{group_name}'")
        print(f"Available groups: {', '.join(STAGE_GROUPS.keys())}")
        sys.exit(1)

    group = STAGE_GROUPS[group_name]
    stages = resolve_stages(group_name, stage_filter)

    if not stages:
        print("Error: No valid stages specified")
        sys.exit(1)

    # Determine output filename
    if stage_filter:
        # Custom filename for filtered export
        filter_suffix = stage_filter.replace(',', '_').replace(' ', '')
        output_file = f"{group_name}_{filter_suffix}_leads.json"
    else:
        output_file = group['output_file']

    print(f"Exporting: {group['description']}")
    print(f"Stages: {', '.join(stages)}")
    print("=" * 60)

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), 'test_exports')
    os.makedirs(output_dir, exist_ok=True)

    fetcher = LeadDataFetcher()

    # Fetch leads from all stages in this group
    all_stage_leads = []
    for stage in stages:
        print(f"\nFetching leads in '{stage}'...")
        people = fetcher._make_request('people', {
            'stage': stage,
            'limit': 200
        })

        if people and 'people' in people:
            leads = people['people']
            print(f"  Found {len(leads)} leads")
            for lead in leads:
                lead['_stage'] = stage  # Track source stage
            all_stage_leads.extend(leads)
        else:
            print(f"  No leads found or API error")

    print(f"\nTotal leads across selected stages: {len(all_stage_leads)}")

    if not all_stage_leads:
        print("No leads to export")
        return

    # Fetch and preprocess each lead
    all_leads = []
    for i, lead in enumerate(all_stage_leads):
        person_id = lead['id']
        name = f"{lead.get('firstName', '')} {lead.get('lastName', '')}".strip()
        stage = lead.get('_stage', 'Unknown')
        print(f"  [{i+1}/{len(all_stage_leads)}] Fetching {person_id}: {name} ({stage})...")

        try:
            bundle = fetcher.fetch_lead(person_id, verbose=False)
            if bundle:
                processed = preprocess_bundle(bundle)
                all_leads.append(processed)
        except Exception as e:
            print(f"    Error: {e}")

    # Write combined JSON file
    output_path = os.path.join(output_dir, output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_leads, f, indent=2, default=str)

    print(f"\nExported {len(all_leads)} leads to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Export leads from Follow Up Boss by stage group',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Stage Groups:
  qualified        Pre-contact qualified leads (ACQ - Qualified, Phase 2, Phase 3)
                   Aliases: 1, 2, 3, phase1, phase2, phase3
  lead-management  ABCD nurture stages (A/B/C/D leads)
                   Aliases: A, B, C, D
  pipeline         Active deals (Needs Offer, Contract Sent, Under Contract)
                   Aliases: needs-offer, contract-sent, under-contract

Examples:
  python export_leads.py --group qualified
  python export_leads.py --group qualified --stages 2,3
  python export_leads.py --group lead-management
  python export_leads.py --group lead-management --stages C,D
  python export_leads.py --group pipeline --stages needs-offer
  python export_leads.py --list
        """
    )
    parser.add_argument('--group', '-g', choices=list(STAGE_GROUPS.keys()),
                        help='Stage group to export')
    parser.add_argument('--stages', '-s', type=str,
                        help='Comma-separated list of stages to include (supports aliases)')
    parser.add_argument('--list', '-l', action='store_true',
                        help='List available stage groups and their stages')

    args = parser.parse_args()

    if args.list:
        print("Available stage groups:\n")
        for name, group in STAGE_GROUPS.items():
            print(f"  {name}")
            print(f"    {group['description']}")
            print(f"    Output: {group['output_file']}")
            print(f"    Stages:")
            for stage in group['stages']:
                # Find alias for this stage
                aliases = [k for k, v in group.get('aliases', {}).items() if v == stage]
                alias_str = f" (alias: {', '.join(aliases)})" if aliases else ""
                print(f"      - {stage}{alias_str}")
            print()
        return

    if not args.group:
        parser.print_help()
        sys.exit(1)

    export_leads(args.group, args.stages)


if __name__ == '__main__':
    main()
