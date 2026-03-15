import time
import pythoncom
import logging
import win32com.client
from email_processor.event_handler import InboxEventHandler

# ==========================================
# CONFIGURATION
# ==========================================
# Anonymized target identifier for the data source
TARGET_SOURCE_IDENTIFIER = "Enterprise Ingestion Gateway"
POLL_INTERVAL = 5  # Seconds between synchronization checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def run_ingestion_service(source_name, interval=POLL_INTERVAL):
    """
    Main execution loop that monitors the external source for new data payloads.
    Uses COM initialization for secure integration with the enterprise environment.
    """
    # Required for handling COM objects in a persistent loop
    pythoncom.CoInitialize()
    
    processed_cache = set()
    
    try:
        # Establish connection to the communication client
        client_app = win32com.client.Dispatch("Outlook.Application")
        namespace = client_app.GetNamespace("MAPI")
        
        # Access the high-level data stream (Inbox)
        data_stream = namespace.Folders[source_name].Folders["Inbox"]
        
        # Initialize the event logic gateway
        handler = InboxEventHandler()

        logging.info(f"Ingestion Service active. Monitoring source: '{source_name}'")

        while True:
            # Filter for unprocessed (unread) payloads to optimize resource usage
            incoming_payloads = data_stream.Items.Restrict("[UnRead] = True")
            
            for item in incoming_payloads:
                unique_id = getattr(item, 'EntryID', None)
                
                if unique_id and unique_id not in processed_cache:
                    logging.info(f"System Event: New data detected. Header: {item.Subject}")
                    
                    # Trigger the ETL pipeline
                    handler.process_item(item)
                    
                    # Register as processed to prevent ingestion loops
                    processed_cache.add(unique_id)
            
            # Idle period to reduce CPU overhead
            time.sleep(interval)

    except Exception as e:
        logging.error(f"Ingestion Service Error: {e}")
    finally:
        # Ensure clean release of system resources
        pythoncom.CoUninitialize()

def main():
    """
    Service entry point. Performs a handshake with the target source 
    and initiates the persistent monitoring loop.
    """
    logging.info("Starting Enterprise Data Ingestion Service...")

    # Preliminary connection check (Handshake)
    pythoncom.CoInitialize()
    try:
        client_app = win32com.client.Dispatch("Outlook.Application")
        namespace = client_app.GetNamespace("MAPI")
        _ = namespace.Folders[TARGET_SOURCE_IDENTIFIER].Folders["Inbox"]
    except Exception as e:
        logging.error(f"Handshake Failed: Unable to reach source '{TARGET_SOURCE_IDENTIFIER}'. Check system permissions. -> {e}")
        return
    finally:
        pythoncom.CoUninitialize()

    logging.info("Handshake Successful. Entering monitoring state.")

    try:
        # Run the ingestion service in the main process
        run_ingestion_service(TARGET_SOURCE_IDENTIFIER)
    except KeyboardInterrupt:
        logging.info("Shutdown Signal Received: Exiting Ingestion Service.")

if __name__ == "__main__":
    main()