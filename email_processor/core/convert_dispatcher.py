import os
import logging

# Import the anonymized converter classes
from email_processor.vendors.vendor_a import VendorAConverter
from email_processor.vendors.vendor_b import VendorBConverter
from email_processor.vendors.vendor_c import VendorCConverter

# Root output directory for extracted data (portfolio-safe relative path)
OUTPUT_DIR = os.path.join(os.getcwd(), "output", "extracted_data")

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------

# A dictionary mapping vendor internal names to their respective classes.
# This avoids complex dynamic imports and keeps the code easy to read and maintain.
CONVERTER_CLASSES = {
    "vendor_a": VendorAConverter,
    "vendor_b": VendorBConverter,
    "vendor_c": VendorCConverter,
}

# Mapping anonymized email domain fragments to our internal vendor names.
# These must match the domains configured in rules.py.
SENDER_MAP = {
    '@vendor-a.com': 'vendor_a',
    '@vendor-b.com': 'vendor_b',
    '@vendor-c.com': 'vendor_c',
}

# -------------------------------------------------------------------------
# CORE LOGIC
# -------------------------------------------------------------------------

def standardize_file(file_path: str, sender_email: str, attachment_name: str = None) -> str | None:
    """
    Identifies the vendor based on the sender's email, initializes the correct
    converter class, and processes the file (PDF or Excel).
    """
    sender_email = (sender_email or '').lower()
    vendor_key = None

    # Step 1: Find the matching vendor key from the email domain
    for domain_fragment, key in SENDER_MAP.items():
        if domain_fragment in sender_email:
            vendor_key = key
            break

    # Step 2: Handle cases where the sender is unknown
    if not vendor_key:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == '.pdf':
            logging.info(f"PDF from unknown sender '{sender_email}' - no converter defined. Keeping original.")
        else:
            logging.info(f"Unknown sender '{sender_email}' ({ext}). Keeping original file.")
        return file_path

    # Step 3: Process the file using the appropriate converter class
    try:
        # Fetch the class reference from our dictionary
        target_converter_class = CONVERTER_CLASSES.get(vendor_key)
        
        if not target_converter_class:
            logging.error(f"Converter class for '{vendor_key}' is not implemented yet.")
            return None

        # Instantiate the class and trigger the processing pipeline
        converter = target_converter_class(
            file_path=file_path, 
            out_dir=OUTPUT_DIR, 
            source_name=attachment_name
        )
        
        # This will automatically read, parse, and save the data to the database
        return converter.convert_and_save()

    except Exception as e:
        logging.error(f"Critical error processing file {file_path} for vendor {vendor_key}: {e}")
        return None