import os
import re
import logging
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
import pdfplumber

from email_processor.utils.date_utils import ensure_date_str
from email_processor.utils.db_utils import upsert_vendor_data

class BaseVendorConverter(ABC):
    """
    Abstract base class for all vendor data converters.
    Handles routine tasks: reading files, standardizing columns, and saving to the database.
    """
    
    # Name of the vendor. Will be overwritten in child classes
    VENDOR_NAME = "UNKNOWN"

    def __init__(self, file_path: str, out_dir: str, source_name: str = None):
        self.file_path = file_path
        self.out_dir = out_dir
        self.source_name = source_name

    # ==========================================
    # Abstract Methods (must be implemented in child classes)
    # ==========================================
    
    @abstractmethod
    def process_pdf(self) -> pd.DataFrame:
        """PDF extraction logic. Must return a pandas DataFrame."""
        pass

    @abstractmethod
    def process_excel(self) -> pd.DataFrame:
        """Excel extraction logic. Must return a pandas DataFrame."""
        pass

    # ==========================================
    # MAIN RUN METHOD
    # ==========================================

    def convert_and_save(self) -> str | None:
        """Detects the file type, runs the appropriate parser, and saves the results to the DB."""
        ext = os.path.splitext(self.file_path)[1].lower()
        df = None

        try:
            # 1. Extract data based on file type
            if ext == ".pdf":
                df = self.process_pdf()
            elif ext in {".xlsx", ".xlsm", ".xls", ".csv"}:
                df = self.process_excel()
            else:
                logging.info(f"Skipped unsupported format: {self.file_path}")
                return self.file_path

            # 2. Enrich and save data if extraction was successful
            if df is not None and not df.empty:
                # Add standard metadata columns if they are not present
                if self.source_name:
                    df["source_file"] = self.source_name
                if "ingestion_timestamp" not in df.columns:
                    df["ingestion_timestamp"] = ensure_date_str(datetime.now())
                
                # Save to database using Upsert logic
                # We use out_dir as the folder to store our SQLite database file
                db_file_path = os.path.join(self.out_dir, "EnterpriseDataWarehouse.db")
                success = upsert_vendor_data(df, db_file_path)
                
                if success:
                    logging.info(f"{self.VENDOR_NAME}: Saved {len(df)} rows to DB ({db_file_path}).")
                    return db_file_path
                else:
                    logging.error(f"{self.VENDOR_NAME}: Failed to save data to DB.")
                    return None
            else:
                logging.warning(f"{self.VENDOR_NAME}: No data extracted from {self.file_path}")
                return None

        except Exception as e:
            logging.exception(f"Failed to convert {self.file_path}: {e}")
            return None

    # ==========================================
    # Common extraction utilities for every vendor
    # ==========================================

    def read_pdf_lines(self) -> list[str]:
        """Common PDF reader that extracts text page by page and returns a list of lines."""
        with pdfplumber.open(self.file_path) as pdf:
            text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
            return text.split("\n")

    def extract_with_pattern(self, lines, keyword, pattern, default="N/A", join_groups=False):
        """Searches for a keyword and extracts data from the next line using a regex pattern."""
        for i, line in enumerate(lines):
            if keyword in line:
                target = lines[i + 1].strip() if i + 1 < len(lines) else ""
                match = re.search(pattern, target)
                if match:
                    if join_groups and match.lastindex and match.lastindex > 1:
                        return "".join(match.groups()).strip()
                    elif join_groups:
                        return match.group(1) + match.group(2)
                    else:
                        return match.group(1).strip()
                else:
                    return target
        return default

    def extract_reference_by_position(self, lines, keyword="REFERENCE_ID", pattern=r"([A-Z0-9]{15,16})\s+1\s*/", default="N/A"):
        """Specific extractor for reference numbers based on line offset."""
        for i, line in enumerate(lines):
            if keyword in line:
                if i + 2 < len(lines):
                    target = lines[i + 2].strip()
                    match = re.search(pattern, target)
                    if match:
                        return match.group(1)
        return default

    def extract_location_from_window(self, lines, known_locations, keyword="Target Location", window=5, default="N/A"):
        """Looks for a specific location within a window of lines after a keyword."""
        places_pattern = r"\b(" + "|".join(known_locations) + r")\b"
        for i, line in enumerate(lines):
            if keyword.lower() in line.lower():
                for j in range(1, window + 1):
                    if i + j < len(lines):
                        target = lines[i + j]
                        match = re.search(places_pattern, target, re.IGNORECASE)
                        if match:
                            return match.group(1).upper()
                return default
        return default