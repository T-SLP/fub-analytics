#!/usr/bin/env python3
"""
Lead LLM Analyzer - Utility to analyze lead data using Claude or ChatGPT

This module provides an interface to send lead data to an LLM and ask questions
about it. It supports both Anthropic (Claude) and OpenAI (ChatGPT) APIs.

Setup:
1. Install the SDK: pip install anthropic  (or: pip install openai)
2. Set your API key in .env:
   ANTHROPIC_API_KEY=sk-ant-...  (for Claude)
   OPENAI_API_KEY=sk-...         (for ChatGPT)

Usage:
    from shared.lead_data_fetcher import LeadDataFetcher, format_bundle_for_llm
    from shared.lead_llm_analyzer import LeadLLMAnalyzer

    # Fetch lead data
    fetcher = LeadDataFetcher()
    bundle = fetcher.fetch_lead(person_id=574888)

    # Analyze with LLM
    analyzer = LeadLLMAnalyzer()  # Uses Claude by default
    response = analyzer.query(bundle, "What is the seller's asking price and motivation level?")
    print(response)
"""

import os
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

# Load .env file from project root
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
except ImportError:
    pass

# Try to import LLM SDKs
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Import the data fetcher
from lead_data_fetcher import LeadDataFetcher, format_bundle_for_llm


class LeadLLMAnalyzer:
    """
    Analyzes lead data using Claude or ChatGPT.

    Usage:
        analyzer = LeadLLMAnalyzer(provider='anthropic')  # or 'openai'
        response = analyzer.query(lead_bundle, "What is the asking price?")
    """

    def __init__(
        self,
        provider: str = 'anthropic',
        api_key: Optional[str] = None,
        model: Optional[str] = None
    ):
        """
        Initialize the analyzer.

        Args:
            provider: 'anthropic' (Claude) or 'openai' (ChatGPT)
            api_key: API key (or set via environment variable)
            model: Model to use (defaults to claude-sonnet-4-20250514 or gpt-4o)
        """
        self.provider = provider.lower()

        if self.provider == 'anthropic':
            if not ANTHROPIC_AVAILABLE:
                raise ImportError(
                    "Anthropic SDK not installed. Run: pip install anthropic"
                )
            self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
            if not self.api_key:
                raise ValueError(
                    "ANTHROPIC_API_KEY not provided. Set it in .env or pass to constructor."
                )
            self.client = anthropic.Anthropic(api_key=self.api_key)
            self.model = model or 'claude-sonnet-4-20250514'

        elif self.provider == 'openai':
            if not OPENAI_AVAILABLE:
                raise ImportError(
                    "OpenAI SDK not installed. Run: pip install openai"
                )
            self.api_key = api_key or os.getenv('OPENAI_API_KEY')
            if not self.api_key:
                raise ValueError(
                    "OPENAI_API_KEY not provided. Set it in .env or pass to constructor."
                )
            self.client = openai.OpenAI(api_key=self.api_key)
            self.model = model or 'gpt-4o'

        else:
            raise ValueError(f"Unknown provider: {provider}. Use 'anthropic' or 'openai'.")

    def _build_system_prompt(self) -> str:
        """Build the system prompt for lead analysis"""
        return """You are a real estate investment analyst assistant. You help analyze lead data from a land acquisition CRM (Follow Up Boss).

Your role is to:
1. Extract specific information from lead notes, calls, and conversation history
2. Identify pricing information (asking prices, offers, valuations)
3. Assess seller motivation and flexibility
4. Identify key property details and any concerns
5. Answer questions about the lead's data accurately

When analyzing notes:
- Pay attention to "Price Objective" fields which contain the seller's asking price
- Look for pricing mentioned in text conversations (e.g., "$39,000 we can talk")
- Note any flexibility signals (e.g., "open to discussion", "negotiable", "make me an offer")
- Identify motivation signals (inherited property, need to sell quickly, divorce, financial pressure)
- Note any deal breakers or concerns

When providing price comparisons:
- County Value = county's assessed value (may include structures/house)
- LI Comp Value = Land Insights estimate (land only, may be lower)
- A seller's asking price BELOW either value could indicate opportunity

Always be specific and cite where you found the information in the data."""

    def query(
        self,
        bundle: Dict[str, Any],
        question: str,
        include_raw: bool = False
    ) -> str:
        """
        Ask a question about a lead's data.

        Args:
            bundle: Lead bundle from LeadDataFetcher
            question: The question to ask
            include_raw: Include raw person record in context

        Returns:
            The LLM's response
        """
        # Format the lead data for the prompt
        lead_context = format_bundle_for_llm(bundle, include_raw=include_raw)

        user_message = f"""Here is the data for a lead:

{lead_context}

---

Question: {question}"""

        if self.provider == 'anthropic':
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                system=self._build_system_prompt(),
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )
            return response.content[0].text

        elif self.provider == 'openai':
            response = self.client.chat.completions.create(
                model=self.model,
                max_tokens=2000,
                messages=[
                    {"role": "system", "content": self._build_system_prompt()},
                    {"role": "user", "content": user_message}
                ]
            )
            return response.choices[0].message.content

    def extract_pricing(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured pricing information from a lead.

        Returns:
            Dictionary with: asking_price, price_source, flexibility, motivation_level, etc.
        """
        question = """Analyze this lead and extract the following information in JSON format:

{
    "asking_price": <number or null if not stated>,
    "asking_price_per_acre": <number or null>,
    "price_source": <"stated directly" | "mentioned in conversation" | "implied" | "not found">,
    "price_flexibility": <"firm" | "negotiable" | "very flexible" | "unknown">,
    "motivation_level": <"low" | "medium" | "high" | "unknown">,
    "motivation_signals": [<list of signals found>],
    "timeline": <"urgent" | "flexible" | "not specified">,
    "concerns_or_red_flags": [<list of any concerns>],
    "key_property_details": {
        "acreage": <number or null>,
        "has_structures": <true | false | "unknown">,
        "road_access": <"yes" | "no" | "unknown">,
        "utilities": <"available" | "not available" | "unknown">
    },
    "summary": <brief 1-2 sentence summary of this lead>
}

Return ONLY the JSON, no other text."""

        response = self.query(bundle, question)

        # Try to parse JSON from response
        try:
            # Handle potential markdown code blocks
            json_str = response
            if '```json' in response:
                json_str = response.split('```json')[1].split('```')[0]
            elif '```' in response:
                json_str = response.split('```')[1].split('```')[0]

            return json.loads(json_str.strip())
        except json.JSONDecodeError:
            return {"raw_response": response, "parse_error": True}

    def compare_to_market(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare the asking price to market values and assess the opportunity.

        Returns:
            Dictionary with price comparison analysis
        """
        mv = bundle.get('market_values', {})
        county_value = mv.get('county_value')
        li_value = mv.get('li_comp_value')

        question = f"""Analyze this lead's pricing relative to market values.

Market Values Available:
- County Value (includes structures): {'$' + f'{county_value:,.0f}' if county_value else 'Not available'}
- Land Insights Comp (land only): {'$' + f'{li_value:,.0f}' if li_value else 'Not available'}

Extract the asking price from the notes/conversations and compare it to these market values.

Return JSON format:
{{
    "asking_price": <number or null>,
    "county_value": {county_value or 'null'},
    "li_comp_value": {li_value or 'null'},
    "vs_county_value": <"below" | "above" | "equal" | "unknown">,
    "vs_li_comp": <"below" | "above" | "equal" | "unknown">,
    "discount_percent_county": <percentage below county value, or null>,
    "discount_percent_li": <percentage below LI comp, or null>,
    "is_potential_opportunity": <true | false>,
    "opportunity_reasoning": <brief explanation>,
    "recommended_action": <your recommendation>
}}

Return ONLY the JSON."""

        response = self.query(bundle, question)

        try:
            json_str = response
            if '```json' in response:
                json_str = response.split('```json')[1].split('```')[0]
            elif '```' in response:
                json_str = response.split('```')[1].split('```')[0]

            return json.loads(json_str.strip())
        except json.JSONDecodeError:
            return {"raw_response": response, "parse_error": True}


def interactive_mode(fetcher: LeadDataFetcher, analyzer: LeadLLMAnalyzer):
    """Run an interactive session to query leads"""
    print("\n" + "=" * 60)
    print("LEAD ANALYZER - Interactive Mode")
    print("=" * 60)
    print("\nCommands:")
    print("  load <person_id>  - Load a lead by ID")
    print("  stage <name>      - Load first lead from a stage")
    print("  ask <question>    - Ask a question about the loaded lead")
    print("  pricing           - Extract structured pricing info")
    print("  compare           - Compare asking price to market value")
    print("  info              - Show current lead info")
    print("  quit              - Exit")
    print("-" * 60)

    current_bundle = None

    while True:
        try:
            user_input = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        parts = user_input.split(maxsplit=1)
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        if command == 'quit' or command == 'exit':
            print("Goodbye!")
            break

        elif command == 'load':
            try:
                person_id = int(args)
                print(f"Loading lead {person_id}...")
                current_bundle = fetcher.fetch_lead(person_id, verbose=True)
                if current_bundle:
                    print(f"Loaded: {current_bundle['lead_info']['name']}")
                    print(f"Stage: {current_bundle['lead_info']['stage']}")
                    print(f"Notes: {current_bundle['summary']['total_notes']}, Calls: {current_bundle['summary']['total_calls']}")
                else:
                    print("Lead not found")
            except ValueError:
                print("Invalid person ID")

        elif command == 'stage':
            stage_name = args
            if not stage_name:
                print("Please provide a stage name")
                continue
            print(f"Loading first lead from '{stage_name}'...")
            bundles = fetcher.fetch_leads_by_stage(stage_name, limit=1, verbose=True)
            if bundles:
                current_bundle = bundles[0]
                print(f"Loaded: {current_bundle['lead_info']['name']}")
            else:
                print("No leads found in that stage")

        elif command == 'ask':
            if not current_bundle:
                print("No lead loaded. Use 'load <person_id>' first.")
                continue
            if not args:
                print("Please provide a question")
                continue
            print("\nAnalyzing...")
            response = analyzer.query(current_bundle, args)
            print("\n" + response)

        elif command == 'pricing':
            if not current_bundle:
                print("No lead loaded. Use 'load <person_id>' first.")
                continue
            print("\nExtracting pricing information...")
            result = analyzer.extract_pricing(current_bundle)
            print("\n" + json.dumps(result, indent=2))

        elif command == 'compare':
            if not current_bundle:
                print("No lead loaded. Use 'load <person_id>' first.")
                continue
            print("\nComparing to market values...")
            result = analyzer.compare_to_market(current_bundle)
            print("\n" + json.dumps(result, indent=2))

        elif command == 'info':
            if not current_bundle:
                print("No lead loaded.")
                continue
            info = current_bundle['lead_info']
            mv = current_bundle['market_values']
            print(f"\nName: {info['name']}")
            print(f"Stage: {info['stage']}")
            print(f"County Value: {'$' + f\"{mv['county_value']:,.0f}\" if mv['county_value'] else 'N/A'}")
            print(f"LI Comp Value: {'$' + f\"{mv['li_comp_value']:,.0f}\" if mv['li_comp_value'] else 'N/A'}")
            print(f"Notes: {current_bundle['summary']['total_notes']}")
            print(f"Calls: {current_bundle['summary']['total_calls']}")

        else:
            print(f"Unknown command: {command}")
            print("Type 'help' for available commands")


# CLI
if __name__ == '__main__':
    # Check which SDKs are available
    if not ANTHROPIC_AVAILABLE and not OPENAI_AVAILABLE:
        print("ERROR: No LLM SDK installed.")
        print("")
        print("To use Claude (recommended):")
        print("  pip install anthropic")
        print("  Then set ANTHROPIC_API_KEY in your .env file")
        print("")
        print("To use ChatGPT:")
        print("  pip install openai")
        print("  Then set OPENAI_API_KEY in your .env file")
        print("")
        print("Get API keys from:")
        print("  Claude: https://console.anthropic.com")
        print("  ChatGPT: https://platform.openai.com")
        sys.exit(1)

    # Determine which provider to use
    provider = 'anthropic' if ANTHROPIC_AVAILABLE else 'openai'

    # Check for API key
    if provider == 'anthropic' and not os.getenv('ANTHROPIC_API_KEY'):
        print("ERROR: ANTHROPIC_API_KEY not set in environment")
        print("Add to your .env file: ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)
    elif provider == 'openai' and not os.getenv('OPENAI_API_KEY'):
        print("ERROR: OPENAI_API_KEY not set in environment")
        print("Add to your .env file: OPENAI_API_KEY=sk-...")
        sys.exit(1)

    print(f"Using {provider.upper()} as LLM provider")

    # Initialize
    fetcher = LeadDataFetcher()
    analyzer = LeadLLMAnalyzer(provider=provider)

    # Check for command line args
    if len(sys.argv) > 1:
        if sys.argv[1] == '--interactive' or sys.argv[1] == '-i':
            interactive_mode(fetcher, analyzer)
        elif sys.argv[1].isdigit():
            # Quick query mode: python lead_llm_analyzer.py <person_id> "question"
            person_id = int(sys.argv[1])
            question = sys.argv[2] if len(sys.argv) > 2 else "What is the asking price and motivation level?"

            print(f"Loading lead {person_id}...")
            bundle = fetcher.fetch_lead(person_id, verbose=True)

            if bundle:
                print(f"\nAnalyzing {bundle['lead_info']['name']}...")
                response = analyzer.query(bundle, question)
                print("\n" + response)
            else:
                print("Lead not found")
        else:
            print("Usage:")
            print("  python lead_llm_analyzer.py --interactive     # Interactive mode")
            print("  python lead_llm_analyzer.py <person_id>       # Quick analysis")
            print("  python lead_llm_analyzer.py <person_id> 'question'")
    else:
        # Default to interactive mode
        interactive_mode(fetcher, analyzer)
