import pandas as pd
from pathlib import Path
import logging
import numpy as np

logger = logging.getLogger(__name__)

class DataProcessor:
    """Process and clean the downloaded Excel file"""

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame, handling multi-row headers"""
        logger.info(f"📖 Reading Excel file: {file_path}")
        try:
            # Get all sheet names
            xls = pd.ExcelFile(file_path, engine='xlrd')
            sheet_names = xls.sheet_names
            logger.info(f"📋 Sheets found: {sheet_names}")

            selected_sheet = None
            header_row_1_idx = None
            df_raw = None

            # Scan all sheets to find one containing 'CPT' and 'DESCRIPTION'
            for sheet in sheet_names:
                df_temp = pd.read_excel(xls, sheet_name=sheet, engine='xlrd', header=None)
                
                for idx, row in df_temp.iterrows():
                    row_str = " ".join(row.astype(str).fillna("")).upper()
                    # Find the first header row
                    if "CPT" in row_str and "DESCRIPTION" in row_str:
                        selected_sheet = sheet
                        header_row_1_idx = idx
                        df_raw = df_temp
                        logger.info(f"✅ Selected sheet: {sheet}")
                        logger.info(f"🔍 Found header row 1 at index: {header_row_1_idx}")
                        break
                if selected_sheet:
                    break

            if selected_sheet is None:
                raise ValueError("Could not find a sheet with 'CPT' and 'DESCRIPTION' headers.")

            # Get the two header rows
            cols_row_1 = df_raw.iloc[header_row_1_idx].fillna('').astype(str).str.strip()
            cols_row_2 = df_raw.iloc[header_row_1_idx + 1].fillna('').astype(str).str.strip()

            # Combine headers
            new_cols = []
            for c1, c2 in zip(cols_row_1, cols_row_2):
                if c1 and "Unnamed" not in c1:
                    new_cols.append(c1)
                elif c2 and "Unnamed" not in c2:
                    new_cols.append(c2)
                else:
                    new_cols.append(c1 if c1 else c2) 
            
            logger.info(f"🛠️ Combined headers: {new_cols}")

            # Find the start of the actual data
            data_start_idx = header_row_1_idx + 2
            for idx, row in df_raw.iloc[data_start_idx:].iterrows():
                first_val = str(row.iloc[0]).strip()
                if first_val and (first_val.startswith("Anes") or first_val.replace('.', '', 1).isdigit()):
                    data_start_idx = idx
                    logger.info(f"🔍 Found data start at index: {data_start_idx}")
                    break
            
            # Create the final DataFrame
            df = df_raw.iloc[data_start_idx:].copy()
            df.columns = new_cols

            logger.info(f"✅ Loaded {len(df)} rows (raw)")
            return df

        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform the raw DataFrame.
        
        Process:
        1. Filter rows where MOD column is empty
        2. Split each row into 2 rows (Facility PIP + Physician PIP)
        3. Map columns to new schema
        """
        logger.info("🧹 Cleaning data...")

        # Find required columns
        columns_mapping = {}
        available_columns = list(df.columns)
        logger.info(f"📋 Columns found: {available_columns}")
        
        # Define what we need to find
        target_cols = {
            'cpt_hcpcs': ['CPT', 'HCPCS'],
            'mod': ['MOD'],
            'description': ['DESCRIPTION'],
            'physicians_fees_north': ['PHYSICIAN', 'NORTH'],
            'asc_fees_north': ['ASC', 'NORTH']
        }
        
        # Find the best match for each target column
        for key, keywords in target_cols.items():
            if key in columns_mapping:
                continue
            
            for col in available_columns:
                col_upper_norm = str(col).upper().replace(" ", "").replace("'", "")
                
                # Check if all keywords are in the normalized column name
                if all(keyword in col_upper_norm for keyword in keywords):
                    # Specific check for physicians_fees_north to avoid matching ASC
                    if key == 'physicians_fees_north' and 'ASC' in col_upper_norm:
                        continue
                    
                    columns_mapping[key] = col
                    break

        logger.info(f"📊 Column mapping: {columns_mapping}")

        # Check required columns
        required_keys = ['cpt_hcpcs', 'mod', 'description', 'physicians_fees_north', 'asc_fees_north']
        missing_keys = [key for key in required_keys if key not in columns_mapping]

        if missing_keys:
            raise ValueError(
                f"Missing required columns: {missing_keys}. Available columns: {list(df.columns)}"
            )

        # Select required columns
        df_selected = df[[columns_mapping[key] for key in required_keys]].copy()
        df_selected.columns = required_keys

        # Step 1: Filter rows where MOD is empty
        logger.info("🔍 Filtering rows where MOD column is empty...")
        initial_count = len(df_selected)
        
        # MOD must be empty (NaN or empty string)
        df_selected['mod'] = df_selected['mod'].astype(str).str.strip()
        df_filtered = df_selected[
            (df_selected['mod'] == '') | 
            (df_selected['mod'] == 'nan') |
            (df_selected['mod'].isna())
        ].copy()
        
        logger.info(f"   Rows before MOD filter: {initial_count}")
        logger.info(f"   Rows after MOD filter: {len(df_filtered)}")

        # Drop empty CPT rows
        df_filtered = df_filtered.dropna(subset=["cpt_hcpcs"])
        df_filtered = df_filtered[df_filtered["cpt_hcpcs"].astype(str).str.strip().str.len() > 0]
        
        logger.info(f"   Rows after CPT cleanup: {len(df_filtered)}")

        # Step 2: Split each row into 2 rows
        logger.info("✂️ Splitting rows into Facility PIP and Physician PIP...")
        
        rows_list = []
        
        for _, row in df_filtered.iterrows():
            code = str(row['cpt_hcpcs']).strip()
            description = str(row['description']).strip() if pd.notna(row['description']) else None
            asc_fee = row['asc_fees_north']
            physician_fee = row['physicians_fees_north']
            
            # Convert fees to numeric, handling errors
            asc_fee = pd.to_numeric(asc_fee, errors='coerce')
            physician_fee = pd.to_numeric(physician_fee, errors='coerce')
            
            # ✅ CRITICAL FIX: Convert numpy nan to Python None
            # This ensures JSON serialization works properly
            asc_fee_value = None if pd.isna(asc_fee) else float(asc_fee)
            physician_fee_value = None if pd.isna(physician_fee) else float(physician_fee)
            
            # Create Facility PIP row
            facility_row = {
                'code': code,
                'code_description': description,
                '80th': asc_fee_value,
                'data_type': 'Facility PIP'
            }
            rows_list.append(facility_row)
            
            # Create Physician PIP row
            physician_row = {
                'code': code,
                'code_description': description,
                '80th': physician_fee_value,
                'data_type': 'Physician PIP'
            }
            rows_list.append(physician_row)
        
        # Create new DataFrame from split rows
        df_final = pd.DataFrame(rows_list)
        
        logger.info(f"✅ Split complete: {len(df_filtered)} rows -> {len(df_final)} rows")
        
        # Reset index
        df_final.reset_index(drop=True, inplace=True)
        
        logger.info(f"✅ Cleaned data: {len(df_final)} rows total")
        logger.info(f"📋 Sample data:\n{df_final.head(10).to_string()}")
        
        return df_final