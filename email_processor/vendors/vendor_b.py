import os
import re
import logging
import pandas as pd
from typing import Optional

from email_processor.core.base_converter import BaseVendorConverter
from email_processor.utils.date_utils import ensure_date_str

class VendorBConverter(BaseVendorConverter):
    """
    Converter for Vendor B. 
    Demonstrates advanced handling of unstructured Excel files with dynamic headers 
    and multi-page PDF regex extraction.
    """
    
    VENDOR_NAME = "VENDOR_B"

    # ==========================================
    # PDF Processing Logic
    # ==========================================
    def process_pdf(self) -> pd.DataFrame:
        """Vendor B specific PDF parsing logic utilizing line-by-line regex extraction."""
        lines = self.read_pdf_lines()

        data = {
            'reference_id': self._extract_primary_reference(lines),
            'group_name':   self._extract_group_entity(lines),
            'vendor_name':  self.VENDOR_NAME,
            'target_date':  self._extract_scheduled_date(lines),
            'primary_location': self._extract_main_location(lines),
            'document_no':  self._extract_doc_id(lines),
            'secondary_location': self._extract_target_destination(lines),
            'item_type':    self._extract_category_code(lines),
        }

        return pd.DataFrame([data])

    def _extract_primary_reference(self, lines) -> str:
        for i, line in enumerate(lines):
            if "primary reference code" in line.lower():
                if i + 1 < len(lines):
                    match = re.search(r'([A-Z]{4}\d{7})', lines[i + 1])
                    if match: return match.group(1)
        return "N/A"

    def _extract_group_entity(self, lines) -> str:
        # Specialized regex for complex entity names with mixed characters
        pattern = re.compile(r"^(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))$", re.IGNORECASE)
        for i, line in enumerate(lines):
            if "entity name" in line.lower():
                if i + 1 < len(lines):
                    parts = lines[i + 1].strip().split()
                    if len(parts) < 2: return " ".join(parts)
                    entity = parts[:2]
                    idx = 2
                    while idx < len(parts) and pattern.match(parts[idx]):
                        entity.append(parts[idx])
                        idx += 1
                    return " ".join(entity)
        return "N/A"

    def _extract_scheduled_date(self, lines) -> str:
        for i, line in enumerate(lines):
            if "estimated completion date" in line.lower():
                if i + 1 < len(lines):
                    match = re.search(r'(\d{2}-[A-Z]{3}-\d{4})', lines[i + 1])
                    if match: return ensure_date_str(match.group(1))
        return "N/A"

    def _extract_main_location(self, lines) -> str:
        for i, line in enumerate(lines):
            if "primary distribution point" in line.lower():  
                for j in range(i + 1, len(lines)):
                    next_line = lines[j].strip()
                    if next_line: return next_line.split()[0]
        return "N/A"

    def _extract_doc_id(self, lines) -> str:
        """Extracts document ID by locating anchor keywords and checking previous line offsets."""
        for i, line in enumerate(lines):
            if "distribution point" in line.lower() and i > 0:
                prev_line = lines[i - 1].strip()
                if prev_line: return prev_line
        return "N/A"

    def _extract_target_destination(self, lines) -> str:
        known_locations = ["LOC_A", "LOC_B", "LOC_C", "LOC_D", "LOC_E", "LOC_F"]
        known_loc_lower = [p.lower() for p in known_locations]
        for i, line in enumerate(lines):
            if "final destination point" in line.lower():
                if i + 1 < len(lines):
                    next_line_words = lines[i + 1].strip().split()
                    if len(next_line_words) >= 2:
                        target = next_line_words[1]
                        if target.lower() in known_loc_lower:
                            return known_locations[known_loc_lower.index(target.lower())]
        return "N/A"

    def _extract_category_code(self, lines) -> str:
        pattern = re.compile(r"\b\d{2}[A-Z]{2}\b", re.IGNORECASE)
        for i, line in enumerate(lines):
            if "primary reference code" in line.lower():
                if i + 1 < len(lines):
                    next_line_split = lines[i + 1].strip().split()
                    for part in next_line_split:
                        if pattern.fullmatch(part): return part
        return "N/A"

    # ==========================================
    # Excel Processing Logic
    # ==========================================
    def process_excel(self) -> pd.DataFrame:
        """Vendor B specific Excel parsing logic (handling dynamic header positioning)."""
        df_raw = pd.read_excel(self.file_path, header=None, dtype=str)
        
        # 1. Dynamically locate the header row
        header_row_idx = None
        for i, row in df_raw.iterrows():
            if any(isinstance(cell, str) and "final destination" in cell.lower() for cell in row):
                header_row_idx = i
                break
        
        if header_row_idx is None:
            logging.warning(f"{self.VENDOR_NAME}: Could not find table header row in {self.file_path}")
            return pd.DataFrame()

        df = pd.read_excel(self.file_path, header=header_row_idx, dtype=str)
        df.columns = [str(col).strip() for col in df.columns]

        # 2. Extract Group Identity from free-text Message header
        message = ""
        for row in df_raw.itertuples(index=False):
            for cell in row:
                if isinstance(cell, str) and 'the following items' in cell.lower():
                    message = cell.strip()
                    break

        group_val = ""
        if message:
            match = re.search(r'via\s+(.*?)\s+for', message, re.IGNORECASE)
            if match: group_val = match.group(1).strip()
        elif self.source_name:
            words = self.source_name.replace(".xlsx", "").replace(".xls", "").split()
            if len(words) >= 2: group_val = " ".join(words[-2:])
            elif words: group_val = words[-1]

        # 3. Dynamic Column Identification
        ref_col = next((col for col in df.columns if 'ref' in col.lower()), None)
        type_col = next((col for col in df.columns if 'type' in col.lower()), None)

        if not ref_col:
            logging.warning(f"{self.VENDOR_NAME}: No reference column found in {self.file_path}")
            return pd.DataFrame()

        # 4. Filter and Validate Data Rows
        pattern = re.compile(r"\b([A-Z]{4}\d{7})\b", re.IGNORECASE)
        validated_rows_indices = []
        for idx, val in df[ref_col].dropna().items():
            match = pattern.fullmatch(str(val).strip())
            if match:
                validated_rows_indices.append((match.group(1).upper(), idx))

        # 5. Global Field Extraction from Raw Sheet Structure
        date_val = "N/A"
        date_pattern = re.compile(r'(\d{2}[./-]\d{2}[./-]\d{4}|\d{4}-\d{2}-\d{2}|\d{2}-[A-Z]{3}-\d{4})')
        for row in df_raw.itertuples(index=False):
            for cell in row:
                if pd.notna(cell) and 'completion' in str(cell).lower():
                    match = date_pattern.search(str(cell))
                    if match: date_val = ensure_date_str(match.group(1))

        # 6. Metadata Context Extraction
        loc_val = "N/A"
        known_points = ["POINT_ALPHA", "POINT_BETA", "POINT_GAMMA"]
        context_source = (self.source_name if self.source_name else os.path.basename(self.file_path)).lower()
        for point in known_points:
            if point.lower() in context_source or point.lower() in message.lower():
                loc_val = point
                break

        # 7. Batch Extraction of Column Lists
        def extract_col_list(keyword, clean_func):
            for col in df.columns:
                if keyword in col.lower().replace(" ", ""):
                    return [clean_func(row) for row in df[col]]
            return ["N/A"] * len(df)

        secondary_loc_list = extract_col_list("finaldestination", lambda x: str(x).strip() if pd.notna(x) else "N/A")
        doc_no_list = extract_col_list("doc", lambda x: str(x).strip() if pd.notna(x) else "")

        # 8. Assemble Normalized Output DataFrame
        out_rows = []
        for i, (ref_id, idx) in enumerate(validated_rows_indices):
            out_rows.append({
                "reference_id": ref_id,
                "group_name":   group_val,
                "vendor_name":  self.VENDOR_NAME,
                "target_date":  date_val,
                "primary_location": loc_val,
                "document_no":  doc_no_list[idx] if idx < len(doc_no_list) else "",
                "secondary_location": secondary_loc_list[idx] if idx < len(secondary_loc_list) else "N/A",
                "item_type":    df.at[idx, type_col] if type_col and idx in df.index else "N/A",
            })

        return pd.DataFrame(out_rows)

def convert_file(path: str, out_dir: str, source_name: str = None):
    converter = VendorBConverter(path, out_dir, source_name)
    return converter.convert_and_save()