# Standardized "Offers Made" Counting Logic

This document defines the standardized rules for counting "Offers Made" across all reporting mechanisms in the FUB Analytics system. These rules ensure consistency between the Vercel dashboard and the weekly/midweek email reports.

## Overview

The "Offers Made" metric captures both **direct offers** (where the agent correctly moves a lead to the "Offers Made" stage) and **implicit offers** (where an offer was clearly made but the agent skipped the "Offers Made" stage).

## What Counts as an Offer

### 1. Direct Offers
Any transition **TO** `ACQ - Offers Made` stage.

**Example:** A lead moves from `ACQ - Needs Offer` → `ACQ - Offers Made`

### 2. Implicit Offers (Skipped "Offers Made" Stage)

#### Via "Offer Not Accepted"
A transition **TO** `ACQ - Offer Not Accepted` **FROM a pre-offer stage**.

**Rationale:** If a lead receives an "Offer Not Accepted" status directly from a pre-offer stage (skipping "Offers Made"), it means an offer was made but the agent skipped the intermediate stage.

**Example (counts as offer):** `ACQ - Needs Offer` → `ACQ - Offer Not Accepted`
**Example (does NOT count):** `ACQ - Offers Made` → `ACQ - Offer Not Accepted` (already counted when entering Offers Made)
**Example (does NOT count):** `ACQ - Contract Sent` → `ACQ - Offer Not Accepted` (already past offer stage)

#### Via "Contract Sent"
A transition **TO** `ACQ - Contract Sent` **FROM a pre-offer stage**.

**Rationale:** If a contract is sent directly from a pre-offer stage (skipping "Offers Made"), it means an offer was made and accepted, but the agent skipped the "Offers Made" stage.

**Example (counts as offer):** `ACQ - Needs Offer` → `ACQ - Contract Sent`
**Example (does NOT count):** `ACQ - Offers Made` → `ACQ - Contract Sent` (already counted when entering Offers Made)

### Pre-Offer Stages

The following stages are considered "pre-offer" (an offer has NOT been made yet):

- `ACQ - Qualified`
- `ACQ - Needs Offer`
- `Qualified Phase 2 - Day 3 to 2 Weeks`
- `Qualified Phase 3 - 2 Weeks to 4 Weeks`
- `ACQ - Listed on Market`
- `ACQ - Went Cold - Drip Campaign`
- `ACQ - Price Motivated`
- `ACQ - Not Ready to Sell`
- `ACQ - New Lead`
- `ACQ - Contacted`
- `ACQ - Attempted Contact`

### Post-Offer Stages (Do NOT Trigger Implicit Offers)

The following stages indicate an offer was already made:

- `ACQ - Offers Made`
- `ACQ - Contract Sent`
- `ACQ - Under Contract`
- `ACQ - Offer Not Accepted`
- `ACQ - Closed Won`

## 24-Hour Deduplication Rule

**Rule:** One unique lead can only count as **1 offer per 24-hour window**, regardless of the offer type.

**Rationale:** This prevents double-counting when:
- A lead moves through multiple stages quickly on the same day
- An agent makes corrections to stage assignments within the same day

**Multiple Offers Are Valid If:**
- The same lead has offer events that are **24+ hours apart**
- This accounts for real scenarios where a first offer is rejected and a subsequent offer is made later

### Examples

| Lead | Event | Time | Counts? | Reason |
|------|-------|------|---------|--------|
| A | Needs Offer → Offers Made | Mon 10am | ✅ Yes | Direct offer |
| A | Offers Made → Offer Not Accepted | Mon 2pm | ❌ No | From "Offers Made" (not implicit) + within 24hrs |
| A | Needs Offer → Offers Made | Tue 11am | ✅ Yes | 24+ hours later = new offer |
| B | Needs Offer → Offer Not Accepted | Mon 9am | ✅ Yes | Implicit offer (skipped Offers Made) |
| B | Needs Offer → Contract Sent | Mon 11am | ❌ No | Within 24 hours of previous offer event |
| C | Qualified → Offers Made | Wed 3pm | ✅ Yes | Direct offer |
| C | Offers Made → Offer Not Accepted | Thu 10am | ❌ No | From "Offers Made" (already counted) |
| C | Needs Offer → Offers Made | Thu 4pm | ✅ Yes | 24+ hours since first offer |

## Implementation

### Dashboard (JavaScript)
**File:** `dashboard/utils/dataProcessing.js`
**Function:** `countOfferEvents(stageChanges, startDate, endDate)`

This function:
1. Identifies all offer events (direct + implicit)
2. Sorts events by person_id and timestamp
3. Applies 24-hour deduplication per lead
4. Returns count and list of deduplicated offer events

### Email Reports (Python)
**File:** `reports/weekly_agent_report.py`
**Function:** `query_standardized_offer_metrics(start_date, end_date)`

This function:
1. Uses SQL CTEs to identify offer events
2. Applies 24-hour deduplication using window functions
3. Groups by agent and returns offer counts

## Stage Names Reference

| Stage Name | Role in Offer Counting |
|------------|------------------------|
| `ACQ - Offers Made` | Direct offer indicator |
| `ACQ - Offer Not Accepted` | Implicit offer if FROM a pre-offer stage |
| `ACQ - Contract Sent` | Implicit offer if FROM a pre-offer stage |
| `ACQ - Needs Offer` | Pre-offer stage (common "from" stage) |
| `ACQ - Qualified` | Pre-offer stage |
| `Qualified Phase 2/3` | Pre-offer stages |

## Report Timing

### Weekly Report (Monday)
- **Sent:** Monday at 6:00 AM Eastern
- **Covers:** Previous full week (Monday 00:00 to Sunday 23:59 Eastern)

### Midweek Report (Wednesday)
- **Sent:** Wednesday at 2:00 PM Eastern
- **Covers:** Current week so far (Monday 00:00 Eastern through 2:00 PM Wednesday Eastern)
- **Note:** Captures all transitions up to the moment the report runs

## Changelog

- **2026-02-04:** Initial implementation of standardized counting logic
  - Added implicit offer detection for "Offer Not Accepted" and "Contract Sent"
  - Implemented 24-hour deduplication rule
  - Synchronized logic between dashboard and email reports
