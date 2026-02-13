#!/usr/bin/env python3
"""
FUB Webhook Server v2.1
Synchronous webhook server for capturing FUB stage changes to Supabase.
Deployed on Railway via nixpacks.toml.
"""

import os
import datetime
from typing import Dict, Optional, Any
from collections import deque
from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import requests

# Configuration
FUB_API_KEY = os.getenv("FUB_API_KEY")
FUB_SYSTEM_KEY = os.getenv("FUB_SYSTEM_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "https://fub-stage-tracker-production.up.railway.app")

app = Flask(__name__)

def extract_lead_source_tag(tags):
    """
    Extract lead source tag from tags array.
    Only two lead sources: ReadyMode (cold calls) and Text Lead (everything else).
    """
    if not tags or not isinstance(tags, list):
        return "Text Lead"

    # Convert all tags to lowercase for case-insensitive matching
    tags_lower = [tag.lower() if isinstance(tag, str) else str(tag).lower() for tag in tags]

    # Check for ReadyMode variations (readymode, ready mode, ready-mode)
    for tag in tags_lower:
        if 'readymode' in tag or 'ready mode' in tag or 'ready-mode' in tag:
            return "ReadyMode"

    # Everything else is a text campaign lead
    return "Text Lead"

class WebhookProcessor:
    """Enhanced webhook processor with race condition protection and lead source extraction"""

    def __init__(self):
        self.stats = {
            'webhooks_received': 0,
            'webhooks_processed': 0,
            'stage_changes_captured': 0,
            'rapid_transitions_captured': 0,
            'webhooks_failed': 0,
            'webhooks_ignored': 0,
            'errors': 0,
            'last_webhook_time': None,
            'system_start_time': datetime.datetime.utcnow(),
            'success_rate': 100.0,
            'webhook_rate_per_hour': 0.0
        }

        print("🚀 Synchronous webhook processor started")

        # Debug storage for recent webhook data
        self.recent_webhook_data = deque(maxlen=10)
        self.last_webhook_inspection = None

    def _extract_person_id(self, webhook_data: Dict[str, Any]) -> Optional[str]:
        """Extract person ID from webhook data - FUB format"""

        print(f"EXTRACTION DEBUG: Input keys = {list(webhook_data.keys())}")

        # CRITICAL: FUB sends resourceIds array - this MUST work
        if 'resourceIds' in webhook_data:
            resource_ids = webhook_data['resourceIds']
            if isinstance(resource_ids, list) and len(resource_ids) > 0:
                # Always take the first resource ID
                person_id = str(resource_ids[0])
                print(f"EXTRACTED person ID from resourceIds: {person_id}")
                return person_id
            else:
                print(f"ERROR: resourceIds exists but is not valid list: {resource_ids}")

        # BACKUP: Try URI query parameter extraction
        if 'uri' in webhook_data and '?id=' in webhook_data['uri']:
            person_id = webhook_data['uri'].split('?id=')[1].split('&')[0]
            if person_id and person_id.isdigit():
                print(f"EXTRACTED person ID from URI: {person_id}")
                return person_id

        # Method 1: URI pattern (both /people/{id}/ and ?id={id} query params)
        if 'uri' in webhook_data:
            uri = webhook_data['uri']
            # Try /people/{id}/ pattern first
            if '/people/' in uri:
                person_id = uri.split('/people/')[-1].split('/')[0]
                if person_id and person_id.isdigit():
                    return person_id
            # Try ?id={id} query parameter (FUB format from logs)
            if '?id=' in uri:
                person_id = uri.split('?id=')[1].split('&')[0]
                if person_id and person_id.isdigit():
                    return person_id

        # Method 2: Direct personId field
        if 'personId' in webhook_data:
            return str(webhook_data['personId'])

        # Method 3: person_id field
        if 'person_id' in webhook_data:
            return str(webhook_data['person_id'])

        # Method 4: id field directly
        if 'id' in webhook_data:
            return str(webhook_data['id'])

        # Method 5: data.people array
        if 'data' in webhook_data and 'people' in webhook_data['data']:
            people = webhook_data['data']['people']
            if isinstance(people, list) and len(people) > 0:
                person = people[0]
                if isinstance(person, dict) and 'id' in person:
                    return str(person['id'])

        # Method 6: data.person object
        if 'data' in webhook_data and 'person' in webhook_data['data']:
            person = webhook_data['data']['person']
            if isinstance(person, dict) and 'id' in person:
                return str(person['id'])

        # Method 7: subject field (common in FUB webhooks)
        if 'subject' in webhook_data and isinstance(webhook_data['subject'], dict):
            if 'id' in webhook_data['subject']:
                return str(webhook_data['subject']['id'])

        # Method 8: event field structure (FUB event-based webhooks)
        if 'event' in webhook_data:
            event_data = webhook_data['event']
            if isinstance(event_data, dict):
                # Try event.person.id
                if 'person' in event_data and isinstance(event_data['person'], dict):
                    if 'id' in event_data['person']:
                        return str(event_data['person']['id'])
                # Try event.id directly
                if 'id' in event_data:
                    return str(event_data['id'])

        # Method 9: First-level scan for any field containing person ID patterns
        for key, value in webhook_data.items():
            if key.lower() in ['person_id', 'personid', 'contact_id', 'contactid', 'lead_id', 'leadid']:
                return str(value)
            # Look for numeric ID values that could be person IDs (typical range 1000-999999)
            if key.lower() == 'id' and isinstance(value, (int, str)):
                try:
                    id_num = int(value)
                    if 1000 <= id_num <= 999999:  # Reasonable person ID range
                        return str(id_num)
                except ValueError:
                    pass

        # Method 10: TEMPORARY WEBHOOK INSPECTOR - Log everything and return ANY numeric ID
        inspection_result = {
            'raw_json': webhook_data,
            'keys': list(webhook_data.keys()),
            'found_ids': [],
            'timestamp': datetime.datetime.utcnow().isoformat()
        }

        print(f"🔍 WEBHOOK INSPECTOR - FULL DATA DUMP:")
        print(f"   Raw JSON: {webhook_data}")
        print(f"   Keys: {list(webhook_data.keys())}")

        # Deep scan for ANY numeric values that could be IDs
        def find_all_numeric_values(data, path=""):
            found_ids = []
            if isinstance(data, dict):
                for k, v in data.items():
                    current_path = f"{path}.{k}" if path else k
                    if isinstance(v, (int, str)):
                        try:
                            num_val = int(v)
                            if 1000 <= num_val <= 999999:  # Reasonable ID range
                                found_ids.append((current_path, num_val))
                        except (ValueError, TypeError):
                            pass
                    elif isinstance(v, (dict, list)):
                        found_ids.extend(find_all_numeric_values(v, current_path))
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    found_ids.extend(find_all_numeric_values(item, f"{path}[{i}]"))
            return found_ids

        all_ids = find_all_numeric_values(webhook_data)
        inspection_result['found_ids'] = all_ids

        # Store for debug endpoint access
        self.last_webhook_inspection = inspection_result

        if all_ids:
            print(f"🎯 FOUND POTENTIAL IDs: {all_ids}")
            # Return the first reasonable ID we find
            return str(all_ids[0][1])

        print("❌ NO NUMERIC IDs FOUND IN WEBHOOK")
        return None

    def _process_single_webhook(self, webhook_data: Dict[str, Any]) -> bool:
        """Process a single webhook with enhanced lead source extraction"""
        try:
            person_id = self._extract_person_id(webhook_data)
            if not person_id:
                return False

            # Get person data from FUB API
            person_data = self._get_person_from_fub(person_id)
            if not person_data:
                print(f"❌ Could not fetch person data for ID: {person_id}")
                return False

            # Process stage change with enhanced lead source extraction
            return self.process_person_stage_change(person_data, webhook_data.get('event', 'webhookEvent'))

        except Exception as e:
            print(f"❌ Error processing webhook: {e}")
            return False

    def _get_person_from_fub(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get person data from FUB API with authentication"""
        try:
            import base64
            auth_string = base64.b64encode(f'{FUB_API_KEY}:'.encode()).decode()

            # CRITICAL: FUB API requires ?fields= parameter to return custom fields
            # Without this, custom fields return as None even if populated in FUB
            # We need to request both standard fields AND custom fields explicitly
            # assignedUserId and assignedTo capture the agent assigned at time of stage change
            fields_param = 'id,firstName,lastName,stage,tags,assignedUserId,assignedTo,customCampaignID,customWhoPushedTheLead,customParcelCounty,customParcelState,customParcelZip'

            response = requests.get(
                f'https://api.followupboss.com/v1/people/{person_id}?fields={fields_param}',
                headers={
                    'Authorization': f'Basic {auth_string}',
                    'X-System': 'SynergyFUBLeadMetrics',
                    'X-System-Key': FUB_SYSTEM_KEY,
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                person = data.get('person', data)

                # Log campaign ID capture for debugging
                campaign_id = person.get('customCampaignID')
                if campaign_id:
                    print(f"✅ Campaign ID captured: {campaign_id}")
                else:
                    print(f"⚠️  No Campaign ID for person {person_id}")

                return person
            else:
                print(f"❌ FUB API error {response.status_code} for person {person_id}")
                return None

        except Exception as e:
            print(f"❌ Exception getting person {person_id}: {e}")
            return None

    def process_person_stage_change(self, person_data: Dict[str, Any], event_type: str) -> bool:
        """Process person stage change with SELECT FOR UPDATE protection and enhanced lead source extraction"""
        try:
            conn = psycopg2.connect(SUPABASE_DB_URL, sslmode='require')

            try:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    person_id = str(person_data.get('id', ''))
                    current_stage = person_data.get('stage', 'Unknown')
                    first_name = person_data.get('firstName', 'Unknown')
                    last_name = person_data.get('lastName', 'Unknown')

                    # ENHANCED LEAD SOURCE EXTRACTION WITH DEBUGGING
                    tags = person_data.get('tags', [])
                    lead_source_tag = extract_lead_source_tag(tags)

                    person_name = f"{first_name} {last_name}"
                    print(f"🔍 DEBUG: Processing {person_name} with {len(tags)} tags: {tags}")
                    if lead_source_tag:
                        print(f"✅ LEAD SOURCE EXTRACTED for {person_name}: {lead_source_tag} from tags: {tags}")
                    else:
                        print(f"⚠️  NO LEAD SOURCE found for {person_name}, tags: {tags}")
                        # Extra debugging for unknown sources
                        if tags:
                            print(f"📋 Available tags for analysis: {[str(tag).lower() for tag in tags]}")

                    # SELECT FOR UPDATE to lock person record during stage check
                    cur.execute("""
                        SELECT stage_to, changed_at
                        FROM stage_changes
                        WHERE person_id = %s
                        ORDER BY changed_at DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (person_id,))

                    result = cur.fetchone()
                    last_recorded_stage = result['stage_to'] if result else None

                    # Check if this is actually a stage change
                    if last_recorded_stage == current_stage:
                        print(f"🔄 No stage change for {person_name}: already in {current_stage}")
                        conn.rollback()
                        return False

                    print(f"🎯 STAGE CHANGE DETECTED for {person_name}: {last_recorded_stage or 'NEW'} → {current_stage}")

                    # DUPLICATE PROTECTION: Check if this exact transition happened within the last second
                    # This is a safety net in case webhook deduplication fails
                    cur.execute("""
                        SELECT id, changed_at
                        FROM stage_changes
                        WHERE person_id = %s
                        AND COALESCE(stage_from, 'NULL') = COALESCE(%s, 'NULL')
                        AND stage_to = %s
                        AND changed_at >= NOW() - INTERVAL '1 second'
                        LIMIT 1
                    """, (person_id, last_recorded_stage, current_stage))

                    recent_duplicate = cur.fetchone()
                    if recent_duplicate:
                        print(f"🛡️  DUPLICATE BLOCKED: Same transition detected within 1 second for {person_name}")
                        print(f"   Existing record at: {recent_duplicate['changed_at']}")
                        conn.rollback()
                        return False

                    # Extract assigned agent info
                    # assignedUserId is the ID, assignedTo is an object with name/email
                    assigned_user_id = person_data.get('assignedUserId')
                    assigned_to = person_data.get('assignedTo')
                    assigned_user_name = None
                    if assigned_to and isinstance(assigned_to, dict):
                        assigned_user_name = assigned_to.get('name')
                    elif assigned_to and isinstance(assigned_to, str):
                        assigned_user_name = assigned_to

                    if assigned_user_id or assigned_user_name:
                        print(f"👤 Assigned agent captured: {assigned_user_name} (ID: {assigned_user_id})")

                    # Insert new stage change record with lead source and assigned agent
                    cur.execute("""
                        INSERT INTO stage_changes (
                            person_id, first_name, last_name, stage_from, stage_to,
                            changed_at, received_at, source, lead_source_tag,
                            deal_id, campaign_id, who_pushed_lead, parcel_county, parcel_state, parcel_zip,
                            assigned_user_id, assigned_user_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        person_id,
                        first_name,
                        last_name,
                        last_recorded_stage,
                        current_stage,
                        datetime.datetime.utcnow(),
                        datetime.datetime.utcnow(),
                        f"wh_{event_type}"[:20],  # Truncated to fit varchar(20)
                        lead_source_tag,         # Enhanced lead source extraction
                        person_data.get('dealId'),
                        person_data.get('customCampaignID'),
                        person_data.get('customWhoPushedTheLead'),
                        person_data.get('customParcelCounty'),
                        person_data.get('customParcelState'),
                        person_data.get('customParcelZip'),
                        str(assigned_user_id) if assigned_user_id else None,
                        assigned_user_name
                    ))

                    conn.commit()
                    print(f"✅ STAGE CHANGE SAVED with lead source: {person_name} → {current_stage} (source: {lead_source_tag})")

                    # Track rapid transitions
                    if last_recorded_stage:
                        self.stats['rapid_transitions_captured'] += 1

                    return True

            except Exception as e:
                conn.rollback()
                print(f"❌ Database transaction failed for {person_data.get('firstName', 'Unknown')}: {e}")
                return False
            finally:
                conn.close()

        except Exception as e:
            print(f"❌ Error processing stage change: {e}")
            return False

    def get_health_stats(self) -> Dict[str, Any]:
        """Get current health and statistics"""
        uptime_hours = (datetime.datetime.utcnow() - self.stats['system_start_time']).total_seconds() / 3600

        # Health issues detection
        health_issues = []
        is_healthy = True

        # Check if no webhooks received for over 90 minutes
        if self.stats['last_webhook_time']:
            minutes_since_last = (datetime.datetime.utcnow() - self.stats['last_webhook_time']).total_seconds() / 60
            if minutes_since_last > 90:
                health_issues.append(f"No webhooks for {int(minutes_since_last)} minutes")
                is_healthy = False

        return {
            'status': 'healthy' if is_healthy else 'unhealthy',
            'healthy': is_healthy,
            'message': 'Real-time stage tracking active' if is_healthy else 'Health issues detected',
            'version': '2.1',
            'system_type': 'FUB Webhook Server',
            'uptime_hours': round(uptime_hours, 1),
            'system_start_time': self.stats['system_start_time'].strftime('%a, %d %b %Y %H:%M:%S GMT'),
            'last_webhook_time': self.stats['last_webhook_time'].strftime('%a, %d %b %Y %H:%M:%S GMT') if self.stats['last_webhook_time'] else None,
            'webhooks_received': self.stats['webhooks_received'],
            'webhooks_processed': self.stats['webhooks_processed'],
            'webhooks_failed': self.stats['webhooks_failed'],
            'webhooks_ignored': self.stats['webhooks_ignored'],
            'stage_changes_captured': self.stats['stage_changes_captured'],
            'rapid_transitions_captured': self.stats['rapid_transitions_captured'],
            'success_rate': round(self.stats['success_rate'], 1),
            'webhook_rate_per_hour': round(self.stats['webhook_rate_per_hour'], 1),
            'webhook_url': f"{WEBHOOK_BASE_URL}/webhook/fub/stage-change",
            'health_issues': health_issues,
            'configuration': {
                'database_configured': bool(SUPABASE_DB_URL),
                'fub_api_configured': bool(FUB_API_KEY),
                'fub_system_key_configured': bool(FUB_SYSTEM_KEY),
                'webhook_base_url': WEBHOOK_BASE_URL,
            }
        }

# Global webhook processor instance
webhook_processor = WebhookProcessor()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify(webhook_processor.get_health_stats())

@app.route('/debug/webhooks', methods=['GET'])
def debug_webhook_data():
    """Debug endpoint to show recent webhook data"""
    return jsonify({
        'recent_webhooks': list(webhook_processor.recent_webhook_data),
        'count': len(webhook_processor.recent_webhook_data)
    })

@app.route('/debug/inspection', methods=['GET'])
def debug_last_inspection():
    """Debug endpoint to show last webhook inspection result"""
    return jsonify({
        'last_inspection': webhook_processor.last_webhook_inspection,
        'has_data': webhook_processor.last_webhook_inspection is not None
    })

@app.route('/stats', methods=['GET'])
def get_stats():
    """Detailed statistics endpoint"""
    stats = webhook_processor.get_health_stats()
    return jsonify(stats)

@app.route('/webhook/fub/stage-change', methods=['POST'])
def handle_fub_stage_webhook():
    """Handle FUB stage change webhooks - SYNCHRONOUS PROCESSING"""
    try:
        webhook_data = request.get_json(silent=True)
        if not webhook_data:
            return jsonify({'error': 'No JSON payload'}), 400

        event_type = webhook_data.get('event', 'unknown')
        person_id = webhook_processor._extract_person_id(webhook_data)
        print(f"📡 SYNC PROCESSING: {event_type} for person {person_id}")

        # BYPASS QUEUE - Process immediately to avoid threading issues
        webhook_processor.stats['webhooks_received'] += 1
        webhook_processor.stats['last_webhook_time'] = datetime.datetime.utcnow()

        if not person_id:
            print(f"⚠️  No person ID in webhook: {webhook_data.get('uri', 'no URI')}")
            webhook_processor.stats['webhooks_ignored'] += 1
            return jsonify({
                'status': 'rejected',
                'message': 'No person ID found'
            }), 400

        # Process immediately instead of queuing
        print(f"🚀 PROCESSING IMMEDIATELY: {person_id}")
        success = webhook_processor._process_single_webhook(webhook_data)

        webhook_processor.stats['webhooks_processed'] += 1
        if success:
            webhook_processor.stats['stage_changes_captured'] += 1
            print(f"✅ IMMEDIATE SUCCESS: Webhook processed")
            return jsonify({
                'status': 'processed',
                'success': True
            }), 200
        else:
            webhook_processor.stats['webhooks_failed'] += 1
            print(f"❌ IMMEDIATE FAILURE: Webhook processing failed")
            return jsonify({
                'status': 'failed',
                'success': False
            }), 200

    except Exception as e:
        print(f"❌ Webhook handling error: {e}")
        webhook_processor.stats['errors'] += 1
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Internal server error: {str(e)}'}), 500

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        'service': 'FUB Webhook Server',
        'status': 'running',
        'version': '2.1',
        'endpoints': [
            '/health',
            '/stats',
            '/webhook/fub/stage-change'
        ]
    })

if __name__ == '__main__':
    print("🚀 FUB Webhook Server v2.1")
    print(f"📡 Webhook endpoint: {WEBHOOK_BASE_URL}/webhook/fub/stage-change")
    print(f"🔗 FUB API configured: {'✅' if FUB_API_KEY else '❌'}")
    print(f"💾 Database configured: {'✅' if SUPABASE_DB_URL else '❌'}")

    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)