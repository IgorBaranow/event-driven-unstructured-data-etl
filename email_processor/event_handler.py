import logging

from email_processor.rules import initialize_rules, rule_matches_email
from email_processor.utils.convert_utils import save_and_convert_attachments

# System-level extensions to ignore during the ingestion process
INGESTION_EXCLUSION_LIST = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff'}

class InboxEventHandler:
    """
    Core event gateway for the monitoring system. 
    Intercepts incoming data streams (emails) and triggers the ETL pipeline 
    if the source metadata matches predefined ingestion rules.
    """
    def __init__(self):
        # Load filtering rules into memory once upon initialization
        self.rules = initialize_rules()

    def OnItemAdd(self, item):
        """
        COM event trigger for new items. 
        Entry point for the reactive processing pipeline.
        """
        logging.info("Reactive Trigger: New item detected. Initializing processing...")
        self.process_item(item)

    def process_item(self, item):
        """
        Extracts metadata from the source item and evaluates it against current rules.
        If a match is found, initiates the extraction and normalization process.
        """
        subject = self._safe_attr(item, 'Subject')
        sender = self._get_source_identifier(item)

        logging.info(f"Ingestion Request -> Header: '{subject}' | Source: '{sender}'")

        # Evaluate the source against the rule engine
        for rule in self.rules:
            if rule_matches_email(rule, sender, subject):
                logging.info(f"Rule Logic Matched: Source '{rule.sender}' recognized. Keywords found.")

                # Execute the extraction logic
                processed_outputs = save_and_convert_attachments(
                    item=item,
                    excluded_extensions=INGESTION_EXCLUSION_LIST,
                    sender_email=sender
                )

                if processed_outputs:
                    logging.info(f"INGESTION SUCCESS: Normalized data from '{sender}' persisted to storage.")
                else:
                    logging.info("Ingestion Notice: No valid data payloads identified in the matched source.")
                
                return  # Terminate rule evaluation after the first successful match

        logging.info("Ingestion Filter: No matching rules found for this source. Item ignored.")

    def _safe_attr(self, item, attr_name: str) -> str:
        """Safely retrieves attributes from the underlying COM object with error handling."""
        try:
            return getattr(item, attr_name) or ""
        except Exception as e:
            logging.error(f"Metadata Retrieval Error: Failed to read attribute '{attr_name}' -> {e}")
            return ""

    def _get_source_identifier(self, item) -> str:
        """
        Resolves the primary source identifier (email address).
        Specifically handles internal corporate addressing protocols 
        to ensure standard SMTP resolution.
        """
        sender = self._safe_attr(item, 'SenderEmailAddress')
        
        # Internal Exchange addresses often use non-standard formatting (/O=...)
        # We resolve these to a universal identifier format
        if sender.startswith("/O="):
            try:
                exch_user = item.Sender.GetExchangeUser()
                if exch_user and exch_user.PrimarySmtpAddress:
                    sender = exch_user.PrimarySmtpAddress
            except Exception:
                pass
                
        return sender