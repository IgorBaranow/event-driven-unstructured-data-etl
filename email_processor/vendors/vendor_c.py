import logging
import pandas as pd
import pdfplumber
import re
from typing import Optional

from email_processor.core.base_converter import BaseVendorConverter
from email_processor.utils.date_utils import ensure_date_str

class VendorCConverter(BaseVendorConverter):
    """
    Converter for Vendor C. 
    Demonstrates advanced PDF handling: 
    1. Dynamic document classification (Single vs Group reports).
    2. Direct tabular data extraction from PDF structures.
    """
    
    VENDOR_NAME = "VENDOR_C"

    # ==========================================
    # PDF Processing Logic
    # ==========================================
    def process_pdf(self) -> pd.DataFrame:
        """
        Main PDF entry point. Classifies the document and routes 
        to the appropriate extraction method.
        """
        doc_type = self._classify_document_layout()
        logging.info(f"{self.VENDOR_NAME}: Document layout classified as '{doc_type}'")

        if doc_type == "multi_item_table":
            return self._extract_tabular_data()
        else:
            lines = self.read_pdf_lines()
            return self._extract_standard_form(lines)

    def _classify_document_layout(self) -> str:
        """Determines if the PDF is a complex multi-item table or a standard form."""
        with pdfplumber.open(self.file_path) as pdf:
            # Check internal metadata
            doc_title = pdf.metadata.get("Title", "") if pdf.metadata else ""
            if "group" in doc_title.lower() or "summary" in doc_title.lower():
                return "multi_item_table"

            # Scan initial lines for classification keywords
            lines = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines += text.split("\n")
                if len(lines) >= 25: break
            
            header_sample = " ".join(lines[:25]).lower()
            if "group" in header_sample or "batch" in header_sample:
                return "multi_item_table"
                    
        return "single_item_form"

    def _extract_tabular_data(self) -> pd.DataFrame:
        """Performs structural extraction of tables directly from PDF pages."""
        df = pd.DataFrame()
        with pdfplumber.open(self.file_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    # Identify the correct table by looking for a 'reference' column
                    if table and any("reference" in str(cell).lower() for cell in table[0]):
                        df = pd.DataFrame(table[1:], columns=table[0])
                        break
                if not df.empty: break

        if df.empty:
            logging.warning(f"{self.VENDOR_NAME}: No valid data table found in PDF.")
            return pd.DataFrame()

        # Helper to map dynamic PDF columns to internal schema
        def find_col(keywords):
            for col in df.columns:
                if any(k.lower() in str(col).lower() for k in keywords):
                    return col
            return None

        col_ref = find_col(["reference", "id", "code"])
        col_group = find_col(["entity", "vessel", "group"])
        col_date = find_col(["date", "eta", "scheduled"])
        col_doc = find_col(["document", "bl", "invoice"])
        col_type = find_col(["type", "category"])
        col_loc_sec = find_col(["delivery", "destination"])
        col_loc_pri = find_col(["point", "location", "berth"]) 

        # Metadata extraction (e.g., getting primary location from a specific string suffix)
        primary_loc_series = pd.Series(["N/A"] * len(df), index=df.index)
        if col_loc_pri:
            primary_loc_series = df[col_loc_pri].astype(str).apply(
                lambda x: x.strip().split()[-1] if x.strip() else "N/A"
            )

        def get_safe_series(col_name):
            if col_name: return df[col_name]
            return pd.Series(["N/A"] * len(df), index=df.index)

        # Assemble the clean enterprise DataFrame
        return pd.DataFrame({
            "reference_id": df[col_ref].astype(str).str.replace(" ", "") if col_ref else get_safe_series(None),
            "group_name": get_safe_series(col_group),
            "vendor_name": self.VENDOR_NAME,
            "target_date": df[col_date].apply(ensure_date_str) if col_date else get_safe_series(None),
            "primary_location": primary_loc_series,
            "document_no": get_safe_series(col_doc),
            "secondary_location": get_safe_series(col_loc_sec),
            "item_type": get_safe_series(col_type)
        })

    def _extract_standard_form(self, lines: list[str]) -> pd.DataFrame:
        """Extracts data from standard PDF forms using regex line-by-line logic."""
        data = {
            "reference_id": "N/A", "group_name": "N/A", "vendor_name": self.VENDOR_NAME,
            "target_date": "N/A", "primary_location": "N/A", "document_no": "N/A",
            "secondary_location": "N/A", "item_type": "N/A"
        }

        # 1. Primary Reference ID Extraction
        for line in lines:
            m = re.search(r"\b([A-Z]{4}\d{7})\b", line)
            if m:
                data["reference_id"] = m.group(1)
                break

        # 2. Header and Metadata field extraction
        for i, line in enumerate(lines):
            l = line.strip()
            
            # Document ID
            if re.search(r"Identification Number", l, re.IGNORECASE):
                m = re.search(r"Number\s*:\s*([A-Z0-9]+)", l, re.IGNORECASE)
                if m: data["document_no"] = m.group(1)
                elif i+1 < len(lines): data["document_no"] = lines[i+1].strip().split()[0]
            
            # Group Entity
            if re.search(r"Entity Ref", l, re.IGNORECASE):
                m = re.search(r"Ref\s*:\s*(.*?)(?:\s*Level|$)", l, re.IGNORECASE)
                if m: data["group_name"] = m.group(1).strip()
                elif i+1 < len(lines): data["group_name"] = lines[i+1].strip()
            
            # Scheduled Date
            if re.search(r"Target\s*\.?\s*Date", l, re.IGNORECASE):
                if ':' in l:
                    val = l.split(":", 1)[-1].strip()
                    m = re.search(r"(\d{2}-[A-Z]{3}-\d{2,4})", val)
                    if m: data["target_date"] = ensure_date_str(m.group(1))
            
            # Secondary Location
            if re.search(r"Point of Entry", l, re.IGNORECASE):
                m = re.search(r"Entry\s*:\s*([A-Z0-9/ ]+)", l, re.IGNORECASE)
                if m: data["secondary_location"] = m.group(1).strip().split()[-1]
                elif i+1 < len(lines): data["secondary_location"] = lines[i+1].strip().split()[-1]
            
            # Primary Location Discovery
            if 'Point of Entry' in l and i > 0:
                words = lines[i-1].strip().split()
                if len(words) >= 2: data["primary_location"] = words[-2]

        return pd.DataFrame([data])

    # ==========================================
    # Excel Processing Logic (Template ready for future implementation)
    # ==========================================
    def process_excel(self) -> pd.DataFrame:
        logging.warning(f"{self.VENDOR_NAME}: Excel parsing is not configured for this specific vendor.")
        return pd.DataFrame()

def convert_file(path: str, out_dir: str, source_name: str = None):
    converter = VendorCConverter(path, out_dir, source_name)
    return converter.convert_and_save()