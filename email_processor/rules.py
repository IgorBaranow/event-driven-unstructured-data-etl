class IngestionRule:
    """
    Data structure representing a filtering criteria for incoming data streams.
    Defines which sources and headers should trigger the ETL pipeline.
    """
    def __init__(self, source_id: str, header_keywords: list[str]):
        self.source_id = source_id.lower()
        self.header_keywords = [kw.strip().lower() for kw in header_keywords]

def rule_matches_payload(rule: IngestionRule, source_identifier: str, header_text: str) -> bool:
    """
    Evaluates if an incoming payload matches a specific ingestion rule.
    Requires a match on the source domain AND at least one keyword in the header.
    """
    src_lower = (source_identifier or '').lower()
    hdr_lower = (header_text or '').lower()
    
    return (
        rule.source_id in src_lower
        and any(kw in hdr_lower for kw in rule.header_keywords)
    )

def initialize_rules() -> list[IngestionRule]:
    """
    Factory function that returns a registry of IngestionRule instances.
    Each rule defines a valid ingestion pathway for specific vendor data.
    """
    rules: list[IngestionRule] = []

    # Portfolio-safe configuration (Anonymized Entities)
    # Mapping Format: ("@source-domain.com", ["keyword_1", "keyword_2", ...])
    logic_registry = [
        # Administrative / Testing pathway
        ("@internal-system.com", ["test_payload", "integrity_check", "handshake"]),
        
        # Production Ingestion pathways for Anonymized Vendors
        ("@vendor-a-global.com", ["report_summary", "batch_update", "status_alpha"]),
        ("@vendor-b-services.com", ["data_feed", "scheduled_export", "transaction_log"]),
        ("@vendor-c-logistics.com", ["node_arrival", "route_update", "manifest_v2"]),
        ("@vendor-d-partner.com", ["external_sync", "delta_load", "system_notice"]),
    ]

    for source_id, keywords in logic_registry:
        rules.append(IngestionRule(
            source_id=source_id,
            header_keywords=keywords
        ))

    return rules