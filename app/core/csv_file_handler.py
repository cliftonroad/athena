import csv
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path

class CSVHandler:
    """Handler for processing CSV files with or without headers"""
    
    def __init__(self, config):
        self.config = config
        self.csv_mappings = config.get('csv_mappings', {})
        
    def _get_table_config(self, table_name: str) -> Optional[Dict]:
        """Get CSV configuration for specified table"""
        table_config = self.csv_mappings.get(table_name)
        if not table_config:
            raise ValueError(f"No CSV configuration found for table: {table_name}")
        return table_config
        
    def _convert_data_type(self, value: str, data_type: str) -> Any:
        """Convert string value to specified data type"""
        if value is None or value == '':
            return None
            
        try:
            if data_type.lower() == 'string':
                return str(value).strip()
            elif data_type.lower() == 'integer':
                return int(value)
            elif data_type.lower() == 'float':
                return float(value)
            elif data_type.lower() == 'date':
                return datetime.strptime(value, '%Y-%m-%d').date()
            elif data_type.lower() == 'datetime':
                return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            elif data_type.lower() == 'boolean':
                return value.lower() in ('true', '1', 'yes', 'y')
            else:
                logging.warning(f"Unknown data type {data_type}, treating as string")
                return str(value).strip()
        except Exception as e:
            logging.error(f"Error converting value '{value}' to type {data_type}: {str(e)}")
            raise ValueError(f"Data type conversion error: {str(e)}")

    def process_csv_file(self, file_path: str, table_name: str) -> List[Dict]:
        """Process CSV file and return list of dictionaries matching database schema"""
        table_config = self._get_table_config(table_name)
        file_config = table_config.get('file_config', {})
        column_mappings = table_config.get('column_mappings', [])
        
        if not column_mappings:
            raise ValueError(f"No column mappings defined for table: {table_name}")
            
        # Create a mapping of column index to configuration
        index_to_column = {
            mapping['column_index']: mapping 
            for mapping in column_mappings
        }
        
        processed_data = []
        
        # Read CSV file
        try:
            with open(file_path, 'r', encoding=file_config.get('encoding', 'utf-8')) as csvfile:
                csv_reader = csv.reader(
                    csvfile, 
                    delimiter=file_config.get('delimiter', ',')
                )
                
                # Skip header if exists
                if file_config.get('has_header', False):
                    next(csv_reader)
                
                # Process each row
                for row_num, row in enumerate(csv_reader, start=1):
                    try:
                        record = {}
                        
                        # Process each column according to mapping
                        for col_idx, value in enumerate(row):
                            if col_idx in index_to_column:
                                mapping = index_to_column[col_idx]
                                db_column = mapping['db_column']
                                data_type = mapping['data_type']
                                required = mapping.get('required', False)
                                
                                # Handle required fields
                                if required and (value is None or value.strip() == ''):
                                    raise ValueError(
                                        f"Required field '{db_column}' is empty in row {row_num}"
                                    )
                                
                                # Convert and store value
                                try:
                                    converted_value = self._convert_data_type(value, data_type)
                                    record[db_column] = converted_value
                                except Exception as e:
                                    raise ValueError(
                                        f"Error converting field '{db_column}' in row {row_num}: {str(e)}"
                                    )
                        
                        processed_data.append(record)
                        
                    except Exception as e:
                        logging.error(f"Error processing row {row_num}: {str(e)}")
                        raise
                        
        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {str(e)}")
            raise
            
        return processed_data

    def validate_csv_structure(self, file_path: str, table_name: str) -> bool:
        """Validate CSV file structure against configuration"""
        table_config = self._get_table_config(table_name)
        column_mappings = table_config.get('column_mappings', [])
        
        # Get maximum column index from configuration
        max_col_index = max(
            mapping['column_index'] 
            for mapping in column_mappings
        )
        
        # Read first row to check structure
        try:
            with open(file_path, 'r', encoding=table_config['file_config'].get('encoding', 'utf-8')) as csvfile:
                csv_reader = csv.reader(
                    csvfile, 
                    delimiter=table_config['file_config'].get('delimiter', ',')
                )
                first_row = next(csv_reader)
                
                # Check if file has enough columns
                if len(first_row) <= max_col_index:
                    raise ValueError(
                        f"CSV file has {len(first_row)} columns, but configuration requires "
                        f"{max_col_index + 1} columns"
                    )
                
                return True
                
        except Exception as e:
            logging.error(f"Error validating CSV structure: {str(e)}")
            raise

    def get_sample_data(self, file_path: str, table_name: str, sample_size: int = 5) -> List[Dict]:
        """Get sample of processed records for verification"""
        processed_data = self.process_csv_file(file_path, table_name)
        return processed_data[:sample_size]