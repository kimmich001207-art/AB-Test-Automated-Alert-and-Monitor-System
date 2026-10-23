
"""XPC revenue monitor configuration."""

PRESTO_CONN_ID = 'ads_quality_presto_cmg_svc-ads-quality-ldap'
ALERT_WEBHOOK_URL = '<REDACTED_SLACK_WEBHOOK_URL>'
ALERT_CHANNEL = '#aq-xpc-alerts'
DEDUPE_HOURS = 2
METADATA_LOOKBACK_DAYS = 30

CONTROL_OPTION = 0

# Traffic ramp-up guideline:
# - first 24h since start_ts: treatment traffic must stay <= TRAFFIC_FIRST_24H_THRESHOLD
# - after that:           treatment traffic must stay <= TRAFFIC_HARD_THRESHOLD
TRAFFIC_FIRST_24H_THRESHOLD = 0.03
TRAFFIC_HARD_THRESHOLD = 0.10

# Fire when (control_rev_per_user - treatment_rev_per_user) / control_rev_per_user exceeds this.
REVENUE_DROP_THRESHOLD = 0.05

# Revenue-loss alert denoising thresholds. Values are fractions, not percentages.
# Treatments below 0.1% traffic are suppressed; treatments below 1% traffic
# require a larger revenue drop before an alert is sent.
VERY_LOW_TRAFFIC_THRESHOLD = 0.001
LOW_TRAFFIC_THRESHOLD = 0.01
LOW_TRAFFIC_REVENUE_DROP_THRESHOLD = 0.25

# Skip alerts when the global denominator is smaller than this (guards traffic AND revenue rules).
MIN_GLOBAL_USERS = 1000

# Scheduled reporting cadence:
# - first HOURLY_REPORT_HOURS hours after XPC start: send a business metric report every hour
# - after that: send a business metric report once per day at DAILY_REPORT_HOUR (KST)
HOURLY_REPORT_HOURS = 6
DAILY_REPORT_HOUR = 23

OWNER_SLACK_IDS = {
    'hacheng': 'U03EGAVU21W',
    'hali6': 'U080U647HAT',
    'hazhang7': 'U07CH47MHFC',
    'miqi': 'U098XH6UBCG',
    'vamazalo': 'U05Q8DJPBBK',
    'chhuang21': 'U07LLPDQC3U',
    'ximeng': 'U0BKXLY9E3V',
    'zili1': 'U06RW93NG56',
    'xixiao1': 'U06HC341LRJ',
    'zhshen01': 'W014MFU4HA7',
    'joxie': 'U0BDYB8ELH0',
    'fasun': 'U07THFB7S72',
    'boradnae': 'U0649M57G0N',
}


# PLACEMENT_ID_TO_NAME = {
#     # SRP
#     15:  'srp_ads',
#     19:  'srp_top_banner',
#     51:  'srp_mid_banner',
#     16:  'srp_carousel',
#     # SDP
#     14:  'sdp1',
#     116: 'sdp2',
#     150: 'sdp3',
#     31:  'sdp_bottom_carousel',
#     30:  'thank_you_carousel',
#     33:  'cart_carousel',
#     55:  'sdp_top_banner',
#     53:  'sdp_mid_banner',
#     52:  'sdp_bottom_banner',
#     204: 'sdp_visually_similar_ads',
#     236: 'sdp_hybrid_ads',
#     # RECO
#     13:  'gw_carousel',
#     66:  'related_ads',
#     17:  'c3_listing_ads',
#     54:  'trending_ads',
#     95:  'pac_ads',
#     142: 'mclp_ads',
# }

MONITORED_CONTAINERS = [
    {'container_id': 813, 'placement_ids': [13]},
    {'container_id': 147, 'placement_ids': [15]},
    {'container_id': 153, 'placement_ids': [150, 14, 236]},
    {'container_id': 226, 'placement_ids': [13]},
    {'container_id': 982, 'placement_ids': []},
]
