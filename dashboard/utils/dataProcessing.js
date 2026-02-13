// utils/dataProcessing.js - Complete data processing functions

import { PRE_OFFER_STAGES } from './constants';

// Helper function to get week start (Monday)
// Aligned with weekly_agent_report.py which uses Monday-Sunday weeks
export const getWeekStart = (date) => {
  const d = new Date(date);
  const day = d.getDay(); // 0=Sunday, 1=Monday, ...
  const diff = d.getDate() - ((day + 6) % 7);
  d.setDate(diff);
  d.setHours(0, 0, 0, 0);
  return d;
};

// Helper function to get date range
export const getDateRange = (timeRangeType = 'main', timeRange, customStart = '', customEnd = '') => {
  let selectedTimeRange, selectedCustomStart, selectedCustomEnd;

  // All charts now use main time range - no separate campaign or lead source time ranges
  selectedTimeRange = timeRange;
  selectedCustomStart = customStart;
  selectedCustomEnd = customEnd;

  if (selectedCustomStart && selectedCustomEnd) {
    return {
      start: new Date(selectedCustomStart),
      end: new Date(selectedCustomEnd + 'T23:59:59.999Z')
    };
  }

  const end = new Date();
  const start = new Date();

  switch (selectedTimeRange) {
    case 'current_week':
      const currentWeekStart = getWeekStart(end);
      return { start: currentWeekStart, end };
    case 'last_week':
      const lastWeekEnd = new Date(getWeekStart(end));
      lastWeekEnd.setDate(lastWeekEnd.getDate() - 1);
      lastWeekEnd.setHours(23, 59, 59, 999);
      const lastWeekStart = getWeekStart(lastWeekEnd);
      return { start: lastWeekStart, end: lastWeekEnd };
    case '30d':
      start.setDate(start.getDate() - 30);
      break;
    case '90d':
      start.setDate(start.getDate() - 90);
      break;
    default:
      start.setDate(start.getDate() - 30);
  }
  return { start, end };
};

// Calculate business days (excluding weekends)
export const getBusinessDays = (startDate, endDate) => {
  const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24));
  let businessDays = 0;
  for (let i = 0; i < totalDays; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);
    const dayOfWeek = date.getDay();
    if (dayOfWeek !== 0 && dayOfWeek !== 6) {
      businessDays++;
    }
  }
  return businessDays;
};

/**
 * Standardized "Offers Made" counting logic
 *
 * Counts offers with these rules:
 * 1. Direct offers: Transition TO "ACQ - Offers Made"
 * 2. Implicit offers: Transition TO "ACQ - Offer Not Accepted" FROM a pre-offer stage
 * 3. Implicit offers: Transition TO "ACQ - Contract Sent" FROM a pre-offer stage
 *
 * Pre-offer stages are stages where an offer hasn't been made yet:
 * - ACQ - Qualified, ACQ - Needs Offer, Qualified Phase 2/3, etc.
 *
 * Post-offer stages (where an offer was already made) do NOT trigger implicit offers:
 * - ACQ - Offers Made, ACQ - Contract Sent, ACQ - Under Contract, etc.
 *
 * 24-hour deduplication: One lead can only count as 1 offer per 24-hour window
 * Multiple offers for the same lead are valid if 24+ hours apart
 *
 * @param {Array} stageChanges - Array of stage change records
 * @param {Date} startDate - Start of date range to count (optional, counts all if not provided)
 * @param {Date} endDate - End of date range to count (optional, counts all if not provided)
 * @returns {Object} { count: number, offerEvents: Array }
 */
export const countOfferEvents = (stageChanges, startDate = null, endDate = null) => {
  // Stage constants
  const OFFERS_MADE_STAGE = 'ACQ - Offers Made';
  const OFFER_NOT_ACCEPTED_STAGE = 'ACQ - Offer Not Accepted';
  const CONTRACT_SENT_STAGE = 'ACQ - Contract Sent';

  // Filter to date range if provided
  let filteredChanges = stageChanges;
  if (startDate && endDate) {
    filteredChanges = stageChanges.filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= startDate && changeDate <= endDate;
    });
  }

  // Identify all offer events
  const offerEvents = [];

  filteredChanges.forEach(change => {
    const stageTo = change.stage_to;
    const stageFrom = change.stage_from;

    let isOfferEvent = false;
    let offerType = null;

    // Rule 1: Direct offer - transition TO "ACQ - Offers Made"
    if (stageTo === OFFERS_MADE_STAGE) {
      isOfferEvent = true;
      offerType = 'direct';
    }
    // Rule 2: Implicit offer - TO "Offer Not Accepted" FROM a pre-offer stage
    // (means they skipped "Offers Made" and went straight to rejection)
    else if (stageTo === OFFER_NOT_ACCEPTED_STAGE && PRE_OFFER_STAGES.includes(stageFrom)) {
      isOfferEvent = true;
      offerType = 'implicit_not_accepted';
    }
    // Rule 3: Implicit offer - TO "Contract Sent" FROM a pre-offer stage
    // (means they skipped "Offers Made" and went straight to contract)
    else if (stageTo === CONTRACT_SENT_STAGE && PRE_OFFER_STAGES.includes(stageFrom)) {
      isOfferEvent = true;
      offerType = 'implicit_contract_sent';
    }

    if (isOfferEvent) {
      offerEvents.push({
        person_id: change.person_id,
        first_name: change.first_name,
        last_name: change.last_name,
        stage_from: stageFrom,
        stage_to: stageTo,
        changed_at: change.changed_at,
        offer_type: offerType
      });
    }
  });

  // Sort by person_id, then by timestamp for deduplication
  offerEvents.sort((a, b) => {
    if (a.person_id !== b.person_id) {
      return a.person_id.localeCompare(b.person_id);
    }
    return new Date(a.changed_at) - new Date(b.changed_at);
  });

  // Apply 24-hour deduplication per lead
  const dedupedOfferEvents = [];
  const lastOfferTimeByLead = {};

  offerEvents.forEach(event => {
    const personId = event.person_id;
    const eventTime = new Date(event.changed_at);

    if (!lastOfferTimeByLead[personId]) {
      // First offer for this lead
      dedupedOfferEvents.push(event);
      lastOfferTimeByLead[personId] = eventTime;
    } else {
      // Check if 24+ hours since last offer for this lead
      const lastOfferTime = lastOfferTimeByLead[personId];
      const hoursDiff = (eventTime - lastOfferTime) / (1000 * 60 * 60);

      if (hoursDiff >= 24) {
        // Valid new offer (24+ hours apart)
        dedupedOfferEvents.push(event);
        lastOfferTimeByLead[personId] = eventTime;
      }
      // else: Skip - within 24 hours of previous offer
    }
  });

  // Log for debugging
  console.log(`📊 OFFER COUNTING: ${offerEvents.length} raw events → ${dedupedOfferEvents.length} after 24hr dedup`);
  if (dedupedOfferEvents.length > 0) {
    const directCount = dedupedOfferEvents.filter(e => e.offer_type === 'direct').length;
    const implicitNotAccepted = dedupedOfferEvents.filter(e => e.offer_type === 'implicit_not_accepted').length;
    const implicitContractSent = dedupedOfferEvents.filter(e => e.offer_type === 'implicit_contract_sent').length;
    console.log(`   - Direct (→ Offers Made): ${directCount}`);
    console.log(`   - Implicit (→ Offer Not Accepted, skipped Offers Made): ${implicitNotAccepted}`);
    console.log(`   - Implicit (→ Contract Sent, skipped Offers Made): ${implicitContractSent}`);
  }

  return {
    count: dedupedOfferEvents.length,
    offerEvents: dedupedOfferEvents
  };
};

/**
 * Get offer events for a specific date (for daily bucketing)
 * Uses the same standardized counting logic as countOfferEvents
 */
export const getOfferEventsForDate = (stageChanges, targetDateStr) => {
  // Stage constants
  const OFFERS_MADE_STAGE = 'ACQ - Offers Made';
  const OFFER_NOT_ACCEPTED_STAGE = 'ACQ - Offer Not Accepted';
  const CONTRACT_SENT_STAGE = 'ACQ - Contract Sent';

  // Filter to the target date and identify offer events
  const offerEvents = [];

  stageChanges.forEach(change => {
    // Convert to Eastern Time for date comparison
    const changeDateTime = new Date(change.changed_at);
    const easternDateStr = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(changeDateTime);

    if (easternDateStr !== targetDateStr) {
      return; // Not on target date
    }

    const stageTo = change.stage_to;
    const stageFrom = change.stage_from;

    let isOfferEvent = false;
    let offerType = null;

    if (stageTo === OFFERS_MADE_STAGE) {
      isOfferEvent = true;
      offerType = 'direct';
    } else if (stageTo === OFFER_NOT_ACCEPTED_STAGE && PRE_OFFER_STAGES.includes(stageFrom)) {
      isOfferEvent = true;
      offerType = 'implicit_not_accepted';
    } else if (stageTo === CONTRACT_SENT_STAGE && PRE_OFFER_STAGES.includes(stageFrom)) {
      isOfferEvent = true;
      offerType = 'implicit_contract_sent';
    }

    if (isOfferEvent) {
      offerEvents.push({
        person_id: change.person_id,
        first_name: change.first_name,
        last_name: change.last_name,
        stage_from: stageFrom,
        stage_to: stageTo,
        changed_at: change.changed_at,
        offer_type: offerType
      });
    }
  });

  return offerEvents;
};

// Calculate average time from ACQ - Qualified to ACQ - Offers Made
// Enhanced approach: Include ALL offers made in the period, regardless of when they were qualified
const calculateAvgTimeToOffer = (stageChanges) => {
  console.log('🔍 CALCULATING AVG TIME TO OFFER (enhanced method)');
  
  // Find all offers made in the current period
  const offersInPeriod = stageChanges.filter(change => 
    change.stage_to === 'ACQ - Offers Made'
  );
  
  console.log(`Found ${offersInPeriod.length} offers made in selected period`);
  
  if (offersInPeriod.length === 0) {
    return 0;
  }
  
  // Group all stage changes by person_id to track individual lead journeys
  const leadJourneys = {};
  
  stageChanges.forEach(change => {
    const personId = change.person_id;
    if (!leadJourneys[personId]) {
      leadJourneys[personId] = [];
    }
    leadJourneys[personId].push({
      stage: change.stage_to,
      timestamp: new Date(change.changed_at),
      first_name: change.first_name,
      last_name: change.last_name
    });
  });

  const timesToOffer = [];
  
  // For each offer made in the period, find their qualification time
  offersInPeriod.forEach(offer => {
    const personId = offer.person_id;
    const journey = leadJourneys[personId] || [];
    
    // Sort by timestamp to ensure chronological order
    journey.sort((a, b) => a.timestamp - b.timestamp);
    
    // Find the first time they entered Qualified stage (anywhere in their journey)
    let qualifiedTime = null;
    for (const stage of journey) {
      if (stage.stage === 'ACQ - Qualified' && !qualifiedTime) {
        qualifiedTime = stage.timestamp;
        break;
      }
    }
    
    if (qualifiedTime) {
      const offerTime = new Date(offer.changed_at);
      const timeDiff = (offerTime - qualifiedTime) / (1000 * 60 * 60 * 24);
      
      if (timeDiff >= 0) { // Only count positive time differences
        timesToOffer.push(timeDiff);
        console.log(`✅ ${offer.first_name} ${offer.last_name}: ${Math.round(timeDiff * 10) / 10} days`);
      }
    } else {
      // NOTE: This means they were qualified outside the current data range
      // For now, we'll exclude these, but ideally we'd query a longer period
      console.log(`❌ ${offer.first_name} ${offer.last_name}: No qualification found in current data (likely qualified outside period)`);
    }
  });

  console.log(`📊 Calculated times for ${timesToOffer.length} of ${offersInPeriod.length} offers`);
  
  // Calculate average
  if (timesToOffer.length === 0) {
    console.log('⚠️ No complete journeys found - consider extending date range for this metric');
    return 0;
  }
  
  const avgDays = timesToOffer.reduce((sum, days) => sum + days, 0) / timesToOffer.length;
  const result = Math.round(avgDays * 10) / 10;
  
  console.log(`📈 Average time to offer: ${result} days (from ${timesToOffer.length} completed journeys)`);
  return result;
};

// Calculate average time to offer using FIXED 30-day period for stable metric
const calculateAvgTimeToOffer30Day = (stageChanges) => {
  console.log('🔍 CALCULATING 30-DAY AVG TIME TO OFFER');
  
  // Always use last 30 days for offers, regardless of selected dashboard period
  const today = new Date();
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(today.getDate() - 30);
  
  // Find all offers made in the last 30 days
  const offersIn30Days = stageChanges.filter(change => {
    const changeDate = new Date(change.changed_at);
    return change.stage_to === 'ACQ - Offers Made' && 
           changeDate >= thirtyDaysAgo && 
           changeDate <= today;
  });
  
  console.log(`Found ${offersIn30Days.length} offers made in last 30 days`);
  
  if (offersIn30Days.length === 0) {
    return 0;
  }
  
  // Group all stage changes by person_id to track individual lead journeys
  const leadJourneys = {};
  stageChanges.forEach(change => {
    const personId = change.person_id;
    if (!leadJourneys[personId]) {
      leadJourneys[personId] = [];
    }
    leadJourneys[personId].push({
      stage: change.stage_to,
      timestamp: new Date(change.changed_at),
      first_name: change.first_name,
      last_name: change.last_name
    });
  });

  const timesToOffer = [];
  
  // For each offer made in last 30 days, find their qualification time
  offersIn30Days.forEach(offer => {
    const personId = offer.person_id;
    const journey = leadJourneys[personId] || [];
    
    // Sort by timestamp to ensure chronological order
    journey.sort((a, b) => a.timestamp - b.timestamp);
    
    // Find the first time they entered Qualified stage
    let qualifiedTime = null;
    for (const stage of journey) {
      if (stage.stage === 'ACQ - Qualified' && !qualifiedTime) {
        qualifiedTime = stage.timestamp;
        break;
      }
    }
    
    if (qualifiedTime) {
      const offerTime = new Date(offer.changed_at);
      const timeDiff = (offerTime - qualifiedTime) / (1000 * 60 * 60 * 24);
      
      if (timeDiff >= 0) { // Only count positive time differences
        timesToOffer.push(timeDiff);
        console.log(`✅ ${offer.first_name} ${offer.last_name}: ${Math.round(timeDiff * 10) / 10} days`);
      }
    } else {
      console.log(`❌ ${offer.first_name} ${offer.last_name}: No qualification found (qualified before data range)`);
    }
  });

  console.log(`📊 30-day avg calculated from ${timesToOffer.length} of ${offersIn30Days.length} offers`);
  
  // Calculate average
  if (timesToOffer.length === 0) {
    console.log('⚠️ No complete journeys found in 30-day period');
    return 0;
  }
  
  const avgDays = timesToOffer.reduce((sum, days) => sum + days, 0) / timesToOffer.length;
  const result = Math.round(avgDays * 10) / 10;
  
  console.log(`📈 30-day Average Time to Offer: ${result} days (from ${timesToOffer.length} completed journeys)`);
  return result;
};

// Check if a stage change represents a throwaway lead
const isThrowawayLead = (change) => {
  const qualifiedStages = [
    'ACQ - Qualified',
    'Qualified Phase 2 - Day 3 to 2 Weeks',  // Fixed: capital W
    'Qualified Phase 3 - 2 Weeks to 4 Weeks'  // Fixed: capital W
  ];
  
  const throwawayStages = [
    'ACQ - Price Motivated',
    'ACQ - Not Interested',
    'ACQ - Not Ready to Sell',
    'ACQ - Dead / DNC'  // Fixed: space around slash
  ];
  
  const isThrowaway = qualifiedStages.includes(change.stage_from) && throwawayStages.includes(change.stage_to);
  
  // Debug logging for throwaway leads
  if (isThrowaway) {
    console.log('🗑️ Throwaway lead detected:', {
      from: change.stage_from,
      to: change.stage_to,
      person: `${change.first_name} ${change.last_name}`,
      date: change.changed_at
    });
  }
  
  return isThrowaway;
};

// Calculate pipeline velocity - average days from ACQ - Qualified to ACQ - Under Contract (60 day avg)
const calculatePipelineVelocity60Day = (stageChanges) => {
  // Use fixed 60-day period for stable metric
  const today = new Date();
  const sixtyDaysAgo = new Date();
  sixtyDaysAgo.setDate(today.getDate() - 60);
  
  // Filter for Under Contract transitions in the 60-day period
  const contractsIn60Days = stageChanges.filter(change => {
    const changeDate = new Date(change.changed_at);
    return change.stage_to === 'ACQ - Under Contract' && 
           changeDate >= sixtyDaysAgo && 
           changeDate <= today;
  });
  
  if (contractsIn60Days.length === 0) {
    return 0;
  }
  
  // Group all stage changes by person_id to track individual lead journeys
  const leadJourneys = {};
  stageChanges.forEach(change => {
    const personId = change.person_id;
    if (!leadJourneys[personId]) {
      leadJourneys[personId] = [];
    }
    leadJourneys[personId].push({
      stage: change.stage_to,
      timestamp: new Date(change.changed_at)
    });
  });
  
  const timesToContract = [];
  
  // For each Under Contract transition in 60-day period, find their qualification time
  contractsIn60Days.forEach(contract => {
    const personId = contract.person_id;
    const journey = leadJourneys[personId] || [];
    
    // Sort by timestamp
    journey.sort((a, b) => a.timestamp - b.timestamp);
    
    // Find first qualification
    let qualifiedTime = null;
    for (const stage of journey) {
      if (stage.stage === 'ACQ - Qualified' && !qualifiedTime) {
        qualifiedTime = stage.timestamp;
        break;
      }
    }
    
    if (qualifiedTime) {
      const contractTime = new Date(contract.changed_at);
      const timeDiff = (contractTime - qualifiedTime) / (1000 * 60 * 60 * 24);
      
      if (timeDiff >= 0) {
        timesToContract.push(timeDiff);
      }
    }
  });
  
  // Calculate average
  if (timesToContract.length === 0) {
    return 0;
  }
  
  const avgDays = timesToContract.reduce((sum, days) => sum + days, 0) / timesToContract.length;
  return Math.round(avgDays * 10) / 10; // Round to 1 decimal place
};

// Legacy function - keep for compatibility but not used
const calculatePipelineVelocity = (stageChanges) => {
  return calculatePipelineVelocity60Day(stageChanges);
};

// Fetch real data from API
export const fetchRealData = async (startDate, endDate, businessDays) => {
  console.log('🚀 fetchRealData called');
  try {
    const startDateStr = startDate.toISOString().split('T')[0];
    const endDateStr = endDate.toISOString().split('T')[0];
    
    // Call our API endpoint
    console.log('📡 Making API call with dates:', { startDateStr, endDateStr });
    const response = await fetch('/api/pipeline-data', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        startDate: startDateStr,
        endDate: endDateStr
      })
    });
    
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    
    const responseData = await response.json();
    
    // Handle new response format with stage analysis
    const stageChanges = responseData.stageChanges || responseData; // Fallback for backward compatibility
    const stageAnalysis = responseData.stageAnalysis || [];
    
    console.log(`Fetched ${stageChanges.length} stage changes from API`);
    console.log('🔍 STAGE ANALYSIS - All stage transitions in selected period:');
    stageAnalysis.forEach(analysis => {
      console.log(`  ${analysis.stage_from || 'NULL'} → ${analysis.stage_to}: ${analysis.count} times`);
    });
    
    // Debug: Find ALL transitions to "ACQ - Offers Made" with names
    console.log('\n🎯 ALL OFFERS MADE TRANSITIONS:');
    const offersTransitions = stageChanges.filter(change => change.stage_to === 'ACQ - Offers Made');
    console.log(`Found ${offersTransitions.length} total transitions to "ACQ - Offers Made"`);
    offersTransitions.forEach((offer, index) => {
      console.log(`  ${index + 1}. ${offer.first_name} ${offer.last_name} - ${offer.changed_at} (from: ${offer.stage_from})`);
    });
    
    // Debug: Search for specific known leads that moved to offers made today
    console.log('\n🔍 SEARCHING FOR SPECIFIC KNOWN OFFERS:');
    const knownOffers = ['Kathryn Bishop', 'Ricky Styles', 'Douglas Barbee'];
    knownOffers.forEach(fullName => {
      const [firstName, lastName] = fullName.split(' ');
      const foundTransition = stageChanges.find(change => 
        change.first_name === firstName && 
        change.last_name === lastName && 
        change.stage_to === 'ACQ - Offers Made'
      );
      if (foundTransition) {
        console.log(`  ✅ ${fullName}: FOUND - ${foundTransition.changed_at} (from: ${foundTransition.stage_from})`);
      } else {
        console.log(`  ❌ ${fullName}: NOT FOUND in current data`);
      }
    });
    
    return processSupabaseData(stageChanges, startDate, endDate, businessDays);
    
  } catch (error) {
    console.error('💥 ERROR in fetchRealData - This will trigger the error handler that resets offers to 0:', error);
    console.error('Error details:', error.message, error.stack);
    throw error;
  }
};

// Process Supabase data into dashboard format
export const processSupabaseData = (stageChanges, startDate, endDate, businessDays) => {
  // Filter out obvious bulk import data that causes chart issues
  const cleanedStageChanges = stageChanges.filter(change => {
    // Filter out the specific problematic bulk import timestamps
    const timestamp = change.changed_at;
    
    // Remove bulk imports from 2025-09-08 that end in .732Z or .731Z (thousands of identical records)
    if (timestamp.includes('2025-09-08T23:56:19.732Z') || 
        timestamp.includes('2025-09-08T23:56:19.731Z')) {
      return false;
    }
    
    return true;
  });
  
  const filteredCount = stageChanges.length - cleanedStageChanges.length;
  if (filteredCount > 0) {
    console.log(`🧹 Filtered out ${filteredCount} bulk import records from 2025-09-08`);
  }
  
  // Filter stage changes to only include the requested period for charts/metrics
  // But keep all data for Time to Offer calculation
  const requestedPeriodChanges = cleanedStageChanges.filter(change => {
    const changeDate = new Date(change.changed_at);
    return changeDate >= startDate && changeDate <= endDate;
  });
  
  console.log(`📊 Total data: ${stageChanges.length} changes, Requested period: ${requestedPeriodChanges.length} changes`);
  // Calculate total days inclusive of both start and end dates
  const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
  
  // Create daily buckets (including weekends for charts)
  const dailyData = [];
  for (let i = 0; i < totalDays; i++) {
    const date = new Date(startDate);
    date.setDate(date.getDate() + i);
    
    // Use Eastern Time for consistent date bucket creation
    const easternDateStr = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(date);
    
    dailyData.push({
      date: easternDateStr,
      qualified: 0,
      offers: 0,
      priceMotivated: 0,
      throwawayLeads: 0,
      dateFormatted: new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        month: 'short', 
        day: 'numeric',
        weekday: 'short'
      }).format(date)
    });
  }
  
  console.log(`🗓️  DAILY BUCKETS CREATED: ${dailyData.map(d => d.date).join(', ')}`);
  console.log(`📅 Date range: ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]} (${totalDays} days)`);

  // Debug: Log unique stage transitions to understand data structure
  const stageTransitions = new Set();
  requestedPeriodChanges.forEach(change => {
    if (change.stage_from && change.stage_to) {
      stageTransitions.add(`${change.stage_from} → ${change.stage_to}`);
    }
  });
  console.log('📊 Unique stage transitions in requested period:', Array.from(stageTransitions).slice(0, 10));

  // STANDARDIZED OFFER COUNTING: Get all deduplicated offer events first
  // This includes direct offers + implicit offers (skipped Offers Made stage)
  // with 24-hour deduplication per lead
  const { count: totalOfferCount, offerEvents: dedupedOfferEvents } = countOfferEvents(requestedPeriodChanges);

  // Create a Set of offer event keys for quick lookup during daily bucketing
  const offerEventKeys = new Set(
    dedupedOfferEvents.map(e => `${e.person_id}_${e.changed_at}`)
  );

  // Count stage changes by day and stage (only for requested period)
  requestedPeriodChanges.forEach(change => {
    // Convert to Eastern Time before extracting date to avoid timezone plotting issues
    const changeDateTime = new Date(change.changed_at);

    // Use Intl.DateTimeFormat for more reliable timezone conversion
    const easternDateStr = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(changeDateTime);

    const changeDate = easternDateStr; // Already in YYYY-MM-DD format
    const dayData = dailyData.find(d => d.date === changeDate);

    if (dayData) {
      if (change.stage_to === 'ACQ - Qualified') {
        dayData.qualified++;
      } else if (change.stage_to === 'ACQ - Price Motivated') {
        dayData.priceMotivated++;
      } else if (isThrowawayLead(change)) {
        console.log(`🗑️ Adding throwaway lead to daily bucket: ${change.first_name} ${change.last_name} on ${changeDate} (bucket: ${dayData.date})`);
        dayData.throwawayLeads++;
      }
      // Note: Offers are handled separately below using deduplicated offer events
    } else {
      // Log when a change doesn't have a matching day bucket
      if (isThrowawayLead(change)) {
        console.log(`❌ Throwaway lead EXCLUDED (no day bucket): ${change.first_name} ${change.last_name} on ${changeDate}`);
      }
    }
  });

  // Add deduplicated offer events to daily buckets
  dedupedOfferEvents.forEach(offerEvent => {
    const changeDateTime = new Date(offerEvent.changed_at);
    const easternDateStr = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(changeDateTime);

    const dayData = dailyData.find(d => d.date === easternDateStr);
    if (dayData) {
      console.log(`📅 Adding ${offerEvent.offer_type} offer to daily bucket: ${offerEvent.first_name} ${offerEvent.last_name} on ${easternDateStr} (${offerEvent.stage_from} → ${offerEvent.stage_to})`);
      dayData.offers++;
    }
  });

  // Generate weekly data
  const weeks = new Map();
  dailyData.forEach(day => {
    const date = new Date(day.date);
    const weekStart = getWeekStart(date);
    const weekKey = weekStart.toISOString().split('T')[0];
    
    if (!weeks.has(weekKey)) {
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekEnd.getDate() + 6);
      weeks.set(weekKey, {
        date: weekKey,
        qualified: 0,
        offers: 0,
        priceMotivated: 0,
        throwawayLeads: 0,
        dateFormatted: `${weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${weekEnd.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`
      });
    }

    const weekData = weeks.get(weekKey);
    weekData.qualified += day.qualified;
    weekData.offers += day.offers;
    weekData.priceMotivated += day.priceMotivated;
    weekData.throwawayLeads += day.throwawayLeads;
  });

  const weeklyData = Array.from(weeks.values()).sort((a, b) => new Date(a.date) - new Date(b.date));

  // Calculate totals
  console.log('🧮 STARTING TOTALS CALCULATION');
  console.log('🔍 dailyData structure check:', dailyData.length, 'days');

  // Safe calculations with error handling
  let qualifiedTotal = 0;
  let offersTotal = 0;
  let priceMotivatedTotal = 0;
  let throwawayTotal = 0;

  try {
    qualifiedTotal = dailyData.reduce((sum, day) => sum + (day.qualified || 0), 0);
    console.log('✅ qualifiedTotal calculated:', qualifiedTotal);
  } catch (error) {
    console.error('❌ Error calculating qualifiedTotal:', error);
  }

  // Use the standardized offer count (already calculated with 24hr dedup)
  offersTotal = totalOfferCount;
  console.log('✅ offersTotal calculated (standardized):', offersTotal);

  try {
    priceMotivatedTotal = dailyData.reduce((sum, day) => sum + (day.priceMotivated || 0), 0);
    console.log('✅ priceMotivatedTotal calculated:', priceMotivatedTotal);
  } catch (error) {
    console.error('❌ Error calculating priceMotivatedTotal:', error);
  }

  try {
    throwawayTotal = dailyData.reduce((sum, day) => sum + (day.throwawayLeads || 0), 0);
    console.log('✅ throwawayTotal calculated:', throwawayTotal);
  } catch (error) {
    console.error('❌ Error calculating throwawayTotal:', error);
  }

  console.log('🧮 FINISHED CALCULATING TOTALS');

  console.log('📊 TOTALS CALCULATED:');
  console.log(`  - offersTotal (from daily buckets): ${offersTotal}`);

  // Safe logging for throwawayTotal to prevent crashes
  try {
    console.log('🔍 DEBUG throwawayTotal:', throwawayTotal, typeof throwawayTotal);
    console.log(`  - throwawayTotal (from daily buckets): ${throwawayTotal}`);
  } catch (error) {
    console.error('❌ Error logging throwawayTotal:', error);
    console.log('🔍 throwawayTotal value:', throwawayTotal);
  }

  // Debug: Show daily breakdown for throwaway leads
  try {
    const dailyThrowawayBreakdown = dailyData.filter(day => day.throwawayLeads > 0);
    if (dailyThrowawayBreakdown.length > 0) {
      console.log('📅 Daily throwaway breakdown:');
      dailyThrowawayBreakdown.forEach(day => {
        console.log(`  ${day.date}: ${day.throwawayLeads} throwaway leads`);
      });
    } else {
      console.log('📅 No daily throwaway breakdown - no days with throwaway leads found');
    }
  } catch (error) {
    console.error('❌ Error in daily throwaway breakdown:', error);
  }
  
  // Week comparisons - always calculate based on actual current date for consistency
  const today = new Date();
  // Set today to end of day to include all changes that happened today
  today.setHours(23, 59, 59, 999);
  const currentWeekStart = getWeekStart(new Date());
  // Set week start to beginning of day
  currentWeekStart.setHours(0, 0, 0, 0);
  const lastWeekStart = new Date(currentWeekStart);
  lastWeekStart.setDate(lastWeekStart.getDate() - 7);
  const lastWeekEnd = new Date(currentWeekStart);
  lastWeekEnd.setDate(lastWeekEnd.getDate() - 1);

  let qualifiedThisWeek = 0, qualifiedLastWeek = 0;
  let offersThisWeek = 0, offersLastWeek = 0;
  let priceMotivatedThisWeek = 0, priceMotivatedLastWeek = 0;
  let throwawayThisWeek = 0, throwawayLastWeek = 0;

  // Calculate week comparisons (use filtered data for period-specific metrics)
  const allStageChanges = requestedPeriodChanges;
  
  // Calculate current week totals
  qualifiedThisWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= currentWeekStart && changeDate <= today && change.stage_to === 'ACQ - Qualified';
    }).length;
  
  // Use standardized offer counting for this week
  const { count: offersThisWeekCount, offerEvents: offersThisWeekEvents } = countOfferEvents(
    allStageChanges, currentWeekStart, today
  );
  offersThisWeek = offersThisWeekCount;
  console.log(`🎯 Offers this week (standardized): ${offersThisWeek}`);
  offersThisWeekEvents.forEach(offer => {
    console.log(`  - ${offer.first_name} ${offer.last_name}: ${offer.offer_type} (${offer.stage_from} → ${offer.stage_to})`);
  });
  
  // Debug logging
  console.log(`FRONTEND DEBUG - Current week: ${currentWeekStart.toISOString().split('T')[0]} to ${today.toISOString().split('T')[0]}`);
  console.log(`FRONTEND DEBUG - Total stage changes received: ${allStageChanges.length}`);
  console.log(`FRONTEND DEBUG - Offers this week (standardized): ${offersThisWeek}`);
  console.log(`FRONTEND DEBUG - Offers total (standardized): ${offersTotal}`);
  
  priceMotivatedThisWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= currentWeekStart && changeDate <= today && change.stage_to === 'ACQ - Price Motivated';
    }).length;

  throwawayThisWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= currentWeekStart && changeDate <= today && isThrowawayLead(change);
    }).length;
  
  // Calculate last week totals
  qualifiedLastWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= lastWeekStart && changeDate <= lastWeekEnd && change.stage_to === 'ACQ - Qualified';
    }).length;
  
  // Use standardized offer counting for last week
  const { count: offersLastWeekCount } = countOfferEvents(
    allStageChanges, lastWeekStart, lastWeekEnd
  );
  offersLastWeek = offersLastWeekCount;
  
  priceMotivatedLastWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= lastWeekStart && changeDate <= lastWeekEnd && change.stage_to === 'ACQ - Price Motivated';
    }).length;

  throwawayLastWeek = allStageChanges
    .filter(change => {
      const changeDate = new Date(change.changed_at);
      return changeDate >= lastWeekStart && changeDate <= lastWeekEnd && isThrowawayLead(change);
    }).length;

  // Debug logging for throwaway calculations
  console.log(`🗑️ THROWAWAY WEEKLY CALCULATIONS:`);
  console.log(`  - throwawayThisWeek: ${throwawayThisWeek}`);
  console.log(`  - throwawayLastWeek: ${throwawayLastWeek}`);
  console.log(`  - throwawayTotal (from daily buckets): ${throwawayTotal}`);

  // Process recent activity (last 100, newest first) - only show bar chart stages + throwaway leads
  const barChartStages = [
    'ACQ - Qualified',
    'ACQ - Offers Made', 
    'ACQ - Price Motivated'
  ];
  
  console.log('📋 Activity table will show these stages + throwaway leads:', barChartStages);
  
  const recentActivity = requestedPeriodChanges
    .filter(change => {
      // Show bar chart stages OR throwaway lead transitions
      const isBarChartStage = barChartStages.includes(change.stage_to);
      const isThrowaway = isThrowawayLead(change);

      // Debug: log what's being filtered
      if (!isBarChartStage && !isThrowaway) {
        console.log('🚫 Filtered out stage:', change.stage_to, 'from:', change.stage_from);
      }

      return isBarChartStage || isThrowaway;
    })
    .slice(0, 100)
    .map(change => {
      // Determine lead source with fallback for NULL values
      let leadSource = change.lead_source_tag;

      // Fallback for NULL lead sources - likely Smarter Contact
      if (!leadSource || leadSource === 'null') {
        const campaignId = change.campaign_id || '';
        if (campaignId.includes('GA') || campaignId.includes('NC')) {
          leadSource = 'Smarter Contact';
        } else {
          leadSource = 'Unknown';
        }
      }

      // Group Roor and Smarter Contact as "Text Lead" for display
      if (leadSource === 'Roor' || leadSource === 'Smarter Contact') {
        leadSource = 'Text Lead';
      }

      return {
        name: `${change.first_name || 'Unknown'} ${change.last_name || ''}`.trim(),
        stage: isThrowawayLead(change) ? 'Throwaway Lead' : change.stage_to,
        actual_stage: change.stage_to,  // Keep original stage for reference
        campaign_code: change.campaign_id || 'No Campaign',
        lead_source: leadSource,
        created_at: change.changed_at,
        previous_stage: change.stage_from || 'Unknown'
      };
    });

  // Get unique campaigns for filter dropdown (from requested period)
  const availableCampaigns = [...new Set(requestedPeriodChanges
    .map(change => change.campaign_id)
    .filter(campaign => campaign && campaign !== null)
  )].sort();

  // Add "No Campaign" if some records don't have campaign_id
  if (requestedPeriodChanges.some(change => !change.campaign_id)) {
    availableCampaigns.push('No Campaign');
  }

  // Calculate campaign metrics (from requested period only)
  // Use standardized offer counting for campaign attribution
  const campaignCounts = {};

  // First, count qualified and price motivated from all stage changes
  requestedPeriodChanges.forEach(change => {
    const campaign = change.campaign_id || 'No Campaign';

    if (!campaignCounts[campaign]) {
      campaignCounts[campaign] = {
        qualified: 0,
        offers: 0,
        priceMotivated: 0,
        leads: 0
      };
    }

    if (change.stage_to === 'ACQ - Qualified') {
      campaignCounts[campaign].qualified++;
    } else if (change.stage_to === 'ACQ - Price Motivated') {
      campaignCounts[campaign].priceMotivated++;
    }

    // Count all stage changes as "leads" for this campaign
    campaignCounts[campaign].leads++;
  });

  // Add offers from deduplicated offer events (standardized counting)
  // Need to look up campaign_id from the original stage changes
  dedupedOfferEvents.forEach(offerEvent => {
    // Find the original change to get campaign_id
    const originalChange = requestedPeriodChanges.find(
      c => c.person_id === offerEvent.person_id && c.changed_at === offerEvent.changed_at
    );
    const campaign = originalChange?.campaign_id || 'No Campaign';

    if (!campaignCounts[campaign]) {
      campaignCounts[campaign] = {
        qualified: 0,
        offers: 0,
        priceMotivated: 0,
        leads: 0
      };
    }
    campaignCounts[campaign].offers++;
  });

  const campaignMetrics = Object.entries(campaignCounts)
    .map(([campaign, counts]) => ({
      campaign,
      qualified: counts.qualified,
      offers: counts.offers,
      priceMotivated: counts.priceMotivated,
      leads: counts.leads
    }))
    .filter(campaign => campaign.qualified > 0)  // Only show campaigns with qualified leads
    .sort((a, b) => b.qualified - a.qualified);  // Sort by qualified count (highest first)

  // Calculate lead source metrics INTEGRATED with main data (no separate API call)
  console.log('✅ USING INTEGRATED LEAD SOURCE CALCULATION - NOT separate API call');
  const leadSourceCounts = {};
  requestedPeriodChanges.forEach(change => {
    if (change.stage_to === 'ACQ - Qualified') {
      let source = change.lead_source_tag;

      // Fallback classification using campaign codes for NULL lead_source_tag
      if (!source || source === 'null') {
        const campaignId = change.campaign_id || '';
        console.log(`🔍 NULL FALLBACK: ${change.first_name} ${change.last_name} - campaign_id: "${campaignId}"`);

        // Classify based on campaign patterns - these are likely Smarter Contact leads
        if (campaignId.includes('GA') || campaignId.includes('NC')) {
          source = 'Smarter Contact';
          console.log(`✅ FALLBACK CLASSIFICATION: ${change.first_name} ${change.last_name} -> Smarter Contact (campaign: ${campaignId})`);
        } else {
          source = 'Unknown';
          console.log(`⚠️ STILL UNKNOWN: ${change.first_name} ${change.last_name} - campaign: "${campaignId}"`);
        }
      }

      // Group Roor and Smarter Contact together as "Text Lead" for display
      if (source === 'Roor' || source === 'Smarter Contact') {
        source = 'Text Lead';
      }

      console.log(`🔍 LEAD SOURCE DEBUG: ${change.first_name} ${change.last_name} - lead_source_tag: "${change.lead_source_tag}" -> using: "${source}"`);
      leadSourceCounts[source] = (leadSourceCounts[source] || 0) + 1;
    }
  });

  const leadSourceMetrics = Object.entries(leadSourceCounts).map(([source, count]) => ({
    name: source,
    value: count,
    percentage: 0
  }));

  // Calculate percentages
  const leadSourceTotal = leadSourceMetrics.reduce((sum, item) => sum + item.value, 0);
  leadSourceMetrics.forEach(item => {
    item.percentage = leadSourceTotal > 0 ? Math.round((item.value / leadSourceTotal) * 100) : 0;
  });

  // Debug: Compare lead source total vs main qualified total
  console.log('🔍 LEAD SOURCE vs QUALIFIED TOTAL COMPARISON (v2):');
  console.log(`  - leadSourceTotal (direct count): ${leadSourceTotal}`);
  console.log(`  - qualifiedTotal (daily buckets): ${qualifiedTotal}`);
  console.log(`  - requestedPeriodChanges with ACQ-Qualified: ${requestedPeriodChanges.filter(c => c.stage_to === 'ACQ - Qualified').length}`);
  console.log(`  - Lead source breakdown:`, leadSourceCounts);
  if (leadSourceTotal !== qualifiedTotal) {
    console.warn('⚠️  MISMATCH detected between lead source total and qualified total!');
    console.warn('📊 This means the pie chart and main metric are using different data sources!');
  }

  // Calculate throwaway leads for the selected date range (reliable method)
  // This counts all throwaway transitions within the requested period
  const throwawayForDateRange = requestedPeriodChanges
    .filter(change => isThrowawayLead(change))
    .length;

  console.log('🗑️ THROWAWAY CALCULATION COMPARISON:');
  console.log(`  - throwawayTotal (daily buckets): ${throwawayTotal}`);
  console.log(`  - throwawayForDateRange (direct count): ${throwawayForDateRange}`);
  console.log(`  - throwawayThisWeek: ${throwawayThisWeek}`);
  console.log(`  - throwawayLastWeek: ${throwawayLastWeek}`);

  // Calculate advanced metrics
  const qualifiedToOfferRate = qualifiedTotal > 0 ? Math.round((offersTotal / qualifiedTotal) * 100) : 0;
  const qualifiedToPriceMotivatedRate = qualifiedTotal > 0 ? Math.round((priceMotivatedTotal / qualifiedTotal) * 100) : 0;

  // Calculate real average time to offer (always use 30-day period for stability)
  const avgTimeToOffer = calculateAvgTimeToOffer30Day(cleanedStageChanges);

  // Calculate pipeline velocity - average days from Qualified to Under Contract
  const pipelineVelocity = calculatePipelineVelocity(cleanedStageChanges);

  return {
    dailyMetrics: dailyData,
    weeklyMetrics: weeklyData,
    campaignMetrics,
    leadSourceMetrics,
    summary: {
      qualifiedTotal,
      qualifiedThisWeek,
      qualifiedLastWeek,
      offersTotal,
      offersThisWeek,
      offersLastWeek,
      priceMotivatedTotal,
      priceMotivatedThisWeek,
      priceMotivatedLastWeek,
      throwawayTotal,
      throwawayThisWeek,
      throwawayLastWeek,
      throwawayForDateRange,
      qualifiedAvgPerDay: businessDays > 0 ? Math.round((qualifiedTotal / businessDays) * 10) / 10 : 0,
      offersAvgPerDay: businessDays > 0 ? Math.round((offersTotal / businessDays) * 10) / 10 : 0,
      priceMotivatedAvgPerDay: businessDays > 0 ? Math.round((priceMotivatedTotal / businessDays) * 10) / 10 : 0,
      qualifiedToOfferRate,
      qualifiedToPriceMotivatedRate,
      avgTimeToOffer,
      pipelineVelocity
    },
    recentActivity,
    filteredActivity: recentActivity,
    availableCampaigns
  };
};

// Fetch campaign data separately
// Campaign data is now included in main fetchRealData function - no separate fetch needed

// Lead source data is now included in main fetchRealData function - no separate fetch needed