import re
import pandas as pd
from datetime import datetime, date

def ensure_date_str(val) -> str:
    """
    Converts various date-like inputs to a standardized YYYY-MM-DD string format 
    for database compatibility. Handles datetime objects, strings, and numeric inputs.
    
    Returns:
        str: The formatted date string, or an empty string on failure.
    """
    try:
        if isinstance(val, (datetime, date)):
            return val.strftime("%Y-%m-%d")
        
        if isinstance(val, (int, float)):
            val = str(int(val))
            
        if isinstance(val, str):
            val = val.strip()
            
            # Check against a robust list of common global formats
            for fmt in (
                "%A, %d %b, %Y",      # Saturday, 22 Feb, 2025
                "%A, %d %b %Y",       # Saturday, 22 Feb 2025
                "%d %b, %Y",          # 22 Feb, 2025
                "%d %b %Y",           # 22 Feb 2025
                "%d-%b-%y",
                "%d-%b-%Y",
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%d.%m.%Y",
                "%Y%m%d",
                "%d%m%Y",
                "%d-%m-%Y"
            ):
                try:
                    return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
                except Exception:
                    continue
            
            # Try to extract and parse a date embedded in longer strings 
            # e.g., "Saturday, 22 Feb, 2025 10:00 PM"
            m = re.search(r"(\d{1,2} [A-Za-z]{3},? \d{4})", val)
            if m:
                for fmt in ("%d %b, %Y", "%d %b %Y"):
                    try:
                        return datetime.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
                    except Exception:
                        continue
                        
            # Fallback: Leverage Pandas' powerful date parser for edge cases
            try:
                dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
                if pd.notnull(dt):
                    return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
                
        return ""
        
    except Exception:
        return ""

def parse_vendor_specific_date(raw_date_str: str) -> str:
    """
    Parses highly unstructured vendor date formats (e.g., 'APR. 17. 2025' or 'APR 17 2025') 
    and converts them to the standard YYYY-MM-DD format.
    
    Returns:
        str: The formatted date string, or an empty string on failure.
    """
    month_map = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }
    
    try:
        if not isinstance(raw_date_str, str):
            return ""
            
        # Extracts Month (3 chars), Day (1-2 chars), and Year (4 chars) regardless of punctuation
        m = re.search(r"([A-Z]{3})\.?\s*(\d{1,2})\.?\s*(\d{4})", raw_date_str.upper())
        if not m:
            return ""
            
        mon_abbr = m.group(1)
        day_raw  = m.group(2)
        year_raw = m.group(3)
        
        mon_num = month_map.get(mon_abbr, None)
        if mon_num is None:
            return ""
            
        # Normalize to DD/MM/YYYY and pass through the main formatter
        day_num = day_raw.zfill(2)
        normalized_date = f"{day_num}/{mon_num}/{year_raw}"
        
        return ensure_date_str(normalized_date)
        
    except Exception:
        return ""