export const TIME_RANGES = {
  CURRENT_WEEK: 'current_week',
  LAST_WEEK: 'last_week',
  THIRTY_DAYS: '30d',
  NINETY_DAYS: '90d',
  CUSTOM: 'custom'
};

export const CHART_TYPES = {
  DAILY: 'daily',
  WEEKLY: 'weekly'
};

export const STAGES = {
  QUALIFIED: 'ACQ - Qualified',
  NEEDS_OFFER: 'ACQ - Needs Offer',
  OFFERS_MADE: 'ACQ - Offers Made',
  OFFER_NOT_ACCEPTED: 'ACQ - Offer Not Accepted',
  CONTRACT_SENT: 'ACQ - Contract Sent',
  PRICE_MOTIVATED: 'ACQ - Price Motivated'
};

// Pre-offer stages: stages where an offer hasn't been made yet.
// Implicit offers only count when transitioning FROM one of these stages.
// IMPORTANT: Keep in sync with shared/constants.py pre_offer_stages in
// weekly_agent_report.py query_standardized_offer_metrics()
export const PRE_OFFER_STAGES = [
  'ACQ - Qualified',
  'ACQ - Needs Offer',
  'Qualified Phase 2 - Day 3 to 2 Weeks',
  'Qualified Phase 3 - 2 Weeks to 4 Weeks',
  'ACQ - Listed on Market',
  'ACQ - Went Cold - Drip Campaign',
  'ACQ - Price Motivated',
  'ACQ - Not Ready to Sell',
  'ACQ - New Lead',
  'ACQ - Contacted',
  'ACQ - Attempted Contact'
];

export const PIE_COLORS = ['#2563eb', '#16a34a', '#dc2626', '#ca8a04', '#9333ea', '#c2410c'];