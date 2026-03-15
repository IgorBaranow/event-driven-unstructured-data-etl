import re
import logging
import pandas as pd

from email_processor.core.base_converter import BaseVendorConverter
from email_processor.utils.date_utils import ensure_date_str

class VendorAConverter(BaseVendorConverter):
    
    VENDOR_NAME = "VENDOR_A"
    
    # Genericized location lists for validation and extraction
    KNOWN_LOCATIONS = [
        "LOCATION_ALPHA", "LOCATION_BETA", "LOCATION_GAMMA", "LOCATION_DELTA", 
        "LOCATION_EPSILON", "LOCATION_ZETA", "LOCATION_ETA", "LOCATION_THETA"
    ]
    ALLOWED_PRIMARY_LOCATIONS = ['LOC_P1', 'LOC_P2', 'LOC_P3', 'LOC_P4', 'LOC_P5']
    ALLOWED_SECONDARY_LOCATIONS = ['LOC_S1', 'LOC_S2', 'LOC_S3', 'LOC_S4', 'LOC_S5', 'LOC_S6']

    # ==========================================
    # PDF Processing Logic
    # ==========================================
    def process_pdf(self) -> pd.DataFrame:
        """Vendor A specific PDF parsing logic utilizing regex pattern matching."""
        lines = self.read_pdf_lines()
        
        # Mapping extracted data to the standard enterprise schema
        data = {
            "reference_id": self.extract_with_pattern(lines, "Primary Reference", r"^([A-Z]{4})\s?(\d{7})\b", join_groups=True),
            "group_name": self.extract_with_pattern(lines, "Group Entity Name", r"^([A-Z\s]+)$"),
            "vendor_name": self.VENDOR_NAME,
            "target_date": ensure_date_str(self.extract_with_pattern(lines, "Target Date Estimate", r"(\d{2}\.[A-Z]{3}\.\d{4})")),
            "primary_location": self.extract_with_pattern(lines, "Primary Destination", r"^[A-Z ]+\s+([A-Z ]+)\s+\d{2}\.[A-Z]{3}\.\d{4}"),
            "document_no": self.extract_reference_by_position(lines, keyword="DOCUMENT ID"),
            "secondary_location": self.extract_location_from_window(lines, self.KNOWN_LOCATIONS, keyword="Secondary Destination"),
            "item_type": "N/A"
        }
        
        return pd.DataFrame([data])

    # ==========================================
    # Excel Processing Logic
    # ==========================================
    def process_excel(self) -> pd.DataFrame:
        """Vendor A specific Excel parsing logic with dynamic header detection."""
        # Read raw sheets without headers to locate the actual data table
        sheets = pd.read_excel(self.file_path, sheet_name=None, header=None, dtype=str)
        selected_sheet, header_idx = None, None
        
        for name, df_raw in sheets.items():
            for idx, row in df_raw.iterrows():
                # Dynamically locate the header row by looking for a key column name
                if row.astype(str).str.contains(r'reference', case=False, regex=True).any():
                    selected_sheet, header_idx = name, idx
                    break
            if selected_sheet: break

        if not selected_sheet:
            logging.warning(f"{self.VENDOR_NAME}: No sheet with 'reference' header found")
            return pd.DataFrame()

        # Re-read the specific sheet using the dynamically found header row
        df = pd.read_excel(self.file_path, sheet_name=selected_sheet, header=header_idx, dtype=str)
        
        # Clean up Pandas unnamed default columns
        df = df.loc[:, ~df.columns.str.contains(r'^Unnamed', regex=True)]

        # Helper to safely locate columns using regex variations
        def find_col(pattern):
            return next((col for col in df.columns if isinstance(col, str) and re.search(pattern, col, flags=re.IGNORECASE)), None)

        ref_col       = find_col(r'reference')
        group_col     = find_col(r'group')
        date_col      = find_col(r'target_date')
        primary_col   = find_col(r'primary_loc')
        doc_col       = find_col(r'document')
        secondary_col = find_col(r'secondary_loc')  
        type_col      = find_col(r'item_type')  

        # Normalize into standard enterprise schema
        df_final = pd.DataFrame({
            'reference_id': df[ref_col].astype(str).str.replace(" ", "", regex=False).str.strip() if ref_col else pd.Series(dtype=str),
            'group_name':   df[group_col].fillna('') if group_col else pd.Series(['N/A'] * len(df)),
            'vendor_name':  [self.VENDOR_NAME] * len(df),
            'target_date':  df[date_col].apply(lambda x: ensure_date_str(str(x))) if date_col else pd.Series(['N/A'] * len(df)),
            'primary_location': df[primary_col].fillna('') if primary_col else pd.Series(['N/A'] * len(df)),
            'document_no':  df[doc_col].fillna('') if doc_col else pd.Series(['N/A'] * len(df)),
            'secondary_location': df[secondary_col].fillna('') if secondary_col else pd.Series(['N/A'] * len(df)),
            'item_type':    df[type_col].fillna('') if type_col else pd.Series(['N/A'] * len(df)),
        })

        # Apply vendor-specific business rules and data sanitization
        df_final['secondary_location'] = df_final['secondary_location'].str.upper().str.strip()
        df_final['primary_location'] = df_final['primary_location'].str.upper().str.strip()
        
        # Filter out rows that do not match allowed enterprise locations
        df_final = df_final[
            df_final['secondary_location'].isin(self.ALLOWED_SECONDARY_LOCATIONS) &
            df_final['primary_location'].isin(self.ALLOWED_PRIMARY_LOCATIONS)
        ].reset_index(drop=True)

        return df_final

# Wrapper for backwards compatibility / direct testing
def convert_file(path: str, out_dir: str, source_name: str = None):
    converter = VendorAConverter(path, out_dir, source_name)
    return converter.convert_and_save()