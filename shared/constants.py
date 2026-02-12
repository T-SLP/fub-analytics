"""
Shared constants for FUB Stage Tracker reports.

All report scripts should import from here so threshold/agent changes only need one edit.
"""

from zoneinfo import ZoneInfo

# Eastern timezone for week boundaries and report display
EASTERN_TZ = ZoneInfo("America/New_York")

# Agents to include in reports (others will be filtered out)
INCLUDED_AGENTS = [
    "Dante Hernandez",
    "Madeleine Penales",
]

# Stage names to track (must match exactly what's in the database)
TRACKED_STAGES = [
    "ACQ - Offers Made",
    "ACQ - Contract Sent",
    "ACQ - Under Contract",
    "Closed",
    "ACQ - Closed Won",
]

# Individual stage name constants
STAGE_OFFERS_MADE = "ACQ - Offers Made"
STAGE_CONTRACT_SENT = "ACQ - Contract Sent"
STAGE_UNDER_CONTRACT = "ACQ - Under Contract"
STAGE_CLOSED = "Closed"
STAGE_CLOSED_WON = "ACQ - Closed Won"

# Pre-offer stages: stages where an offer hasn't been made yet.
# Used by query_standardized_offer_metrics() for implicit offer detection.
# IMPORTANT: Keep in sync with dashboard/utils/constants.js PRE_OFFER_STAGES
PRE_OFFER_STAGES = (
    "ACQ - Qualified",
    "ACQ - Needs Offer",
    "Qualified Phase 2 - Day 3 to 2 Weeks",
    "Qualified Phase 3 - 2 Weeks to 4 Weeks",
    "ACQ - Listed on Market",
    "ACQ - Went Cold - Drip Campaign",
    "ACQ - Price Motivated",
    "ACQ - Not Ready to Sell",
    "ACQ - New Lead",
    "ACQ - Contacted",
    "ACQ - Attempted Contact",
)

# Connection threshold: minimum call duration (in seconds) to count as a real connection.
# Calls below this are likely voicemails. Based on data sampling, calls under 3 min
# are still frequently voicemails, so we use 180 seconds (3 minutes).
CONNECTION_THRESHOLD_SECONDS = 180
