# app/core/data_validator.py
from datetime import datetime
import pandas as pd

class DataValidator:
    def __init__(self, db_handler, logger):
        self.db_handler = db_handler
        self.logger = logger
    
    def validate_data(self, df, table_name):
        validation_results = {
            'is_valid': True,
            'errors': []
        }
        
        # Get table columns
        table_cols = self.db_handler.get_table_columns(table_name)
        
        # Check required columns
        # required_cols = ['id', '', 'created_by', 'modified_date', 
        #                 'modified_by', 'load_batch_no']
        # missing_cols = [col for col in required_cols if col not in df.columns]
        # if missing_cols:
        #     validation_results['is_valid'] = False
        #     validation_results['errors'].append(
        #         f"Missing required columns: {missing_cols}"
        #     )
        
        # Check for empty rows
        empty_rows = df.index[df.isnull().all(axis=1)].tolist()
        if empty_rows:
            validation_results['is_valid'] = False
            validation_results['errors'].append(
                f"Empty rows found at indices: {empty_rows}"
            )
        
        # Validate data types and sizes
        for col in df.columns:
            if col in table_cols:
                col_type = table_cols[col]['type']
                
                # Check numeric columns
                if str(col_type).startswith('INTEGER'):
                    non_numeric = df[
                        ~df[col].astype(str).str.match(r'^\d+$')
                    ].index.tolist()
                    if non_numeric:
                        validation_results['is_valid'] = False
                        validation_results['errors'].append(
                            f"Non-numeric values in column {col} at rows: {non_numeric}"
                        )
                
                # Check date columns
                elif str(col_type).startswith('DATE'):
                    try:
                        pd.to_datetime(df[col])
                    except:
                        validation_results['is_valid'] = False
                        validation_results['errors'].append(
                            f"Invalid date format in column {col}"
                        )
                
                # Check string length
                elif str(col_type).startswith('VARCHAR'):
                    max_length = int(str(col_type).split('(')[1].split(')')[0])
                    too_long = df[df[col].astype(str).str.len() > max_length].index.tolist()
                    if too_long:
                        validation_results['is_valid'] = False
                        validation_results['errors'].append(
                            f"Values exceeding max length ({max_length}) in column {col} at rows: {too_long}"
                        )
        
        return validation_results