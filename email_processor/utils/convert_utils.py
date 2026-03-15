import os
import logging
import tempfile

# Standardize function from our OOP dispatcher
from email_processor.core.convert_dispatcher import standardize_file

# Define which file types the ETL pipeline can process
CONVERTIBLE_EXT = {'.csv', '.xls', '.xlsx', '.pdf'}

# Define which file types to ignore (typically email signatures, logos, banners)
DEFAULT_EXCLUDED_EXT = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff'}

def save_and_convert_attachments(item, excluded_extensions=DEFAULT_EXCLUDED_EXT, sender_email: str = None) -> list[str]:
    """
    Iterates through email attachments from an Outlook COM object.
    Saves valid attachments to a secure temporary OS folder, 
    and dispatches them to the core conversion pipeline.
    
    Args:
        item: Outlook MailItem COM object.
        excluded_extensions (set): Set of file extensions to ignore.
        sender_email (str): The extracted sender's email address for vendor routing.
        
    Returns:
        list[str]: A list of file paths (either successfully converted DB paths or raw temp files).
    """
    outputs: list[str] = []
    
    # Safely get the count of attachments (handles cases where the object might lack attachments)
    count = getattr(item.Attachments, 'Count', 0)
    
    for i in range(1, count + 1):
        att = item.Attachments.Item(i)
        name = att.FileName
        ext = os.path.splitext(name)[1].lower()
        
        # Skip noise files like embedded signature images
        if ext in excluded_extensions:
            continue
            
        # Securely create a temporary file that the OS will manage
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            out_path = tmp.name
            
        try:
            # Download the attachment from Exchange/Outlook to the local temp path
            att.SaveAsFile(out_path)
        except Exception as e:
            logging.error(f"Failed to extract attachment '{name}': {e}")
            continue
            
        # If the file format is supported by our pipeline, send it to the Dispatcher
        if ext in CONVERTIBLE_EXT:
            try:
                # Triggers the dynamic routing and ETL process
                converted_db_path = standardize_file(
                    file_path=out_path, 
                    sender_email=sender_email, 
                    attachment_name=name
                )
                
                if converted_db_path:
                    outputs.append(converted_db_path)
                    continue
            except Exception as e:
                logging.error(f"ETL pipeline conversion error for '{out_path}': {e}")
                
        # If not converted (or unsupported format but not excluded), keep the raw path
        outputs.append(out_path)
        
    return outputs