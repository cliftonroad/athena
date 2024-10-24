class DatabaseHandler:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.error_recovery = ErrorRecoveryHandler(self)
        self.csv_handler = CSVHandler(config)
    
    def load_csv_to_table(self, file_path: str, table_name: str, batch_size: int = 1000):
        """Load CSV file into specified table"""
        try:
            # Validate CSV structure
            self.csv_handler.validate_csv_structure(file_path, table_name)
            
            # Process CSV file
            data = self.csv_handler.process_csv_file(file_path, table_name)
            
            # Log sample data
            sample_size = min(5, len(data))
            logging.info(f"Sample of first {sample_size} records:")
            for idx, record in enumerate(data[:sample_size]):
                logging.info(f"Record {idx + 1}: {record}")
            
            # Insert data
            return self.insert_data(table_name, data, batch_size)
            
        except Exception as e:
            logging.error(f"Error loading CSV file: {str(e)}")
            raise

    def preview_csv_load(self, file_path: str, table_name: str, sample_size: int = 5):
        """Preview how CSV data will be loaded"""
        sample_data = self.csv_handler.get_sample_data(file_path, table_name, sample_size)
        columns = self.get_table_columns(table_name)
        
        return {
            "sample_data": sample_data,
            "table_schema": columns,
            "record_count": len(sample_data)
        }