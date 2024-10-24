from sqlalchemy import create_engine, MetaData, inspect, text, Table, event, insert
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from urllib.parse import quote_plus
import logging
import pandas as pd
from datetime import datetime

class DatabaseHandler:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        
    def _create_engine(self):
        """Create database engine with enhanced error handling and connection validation"""
        db_config = self.config.db_config
        
        # Validate configuration
        required_fields = ['type', 'host', 'port', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not db_config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required database configuration fields: {', '.join(missing_fields)}")
            
        # Escape special characters in password
        escaped_password = quote_plus(db_config['password'])
        
        if db_config['type'] == 'postgres':
            conn_str = (
                f"postgresql://{db_config['user']}:{escaped_password}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")
            
        # Log connection attempt (without sensitive info)
        logging.info(
            f"Attempting to connect to {db_config['type']} database at "
            f"{db_config['host']}:{db_config['port']}/{db_config['database']} "
            f"as user {db_config['user']}"
        )
        
        # Create engine with echo for debugging
        engine = create_engine(
            conn_str,
            echo=logging.getLogger().level == logging.DEBUG,  # SQL logging when in debug mode
            pool_pre_ping=True  # Enable connection health checks
        )
        
        # Attach the error logging event listener
        self._attach_error_logging(engine)

        # Verify connection
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logging.info("Database connection successfully established")
        except Exception as e:
            logging.error(f"Failed to connect to database: {str(e)}")
            raise

        return engine

    def _attach_error_logging(self, engine):
        """Attach SQL error logging to engine for detailed error capture."""
        @event.listens_for(engine, "handle_error")
        def receive_handle_error(exception_context):
            logging.error(f"Error during SQL execution: {exception_context.original_exception}")
            logging.error(f"Context: {exception_context.statement}")
            logging.error(f"Parameters: {exception_context.parameters}")
    
    @contextmanager
    def session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Session error: {str(e)}", exc_info=True)
            raise
        finally:
            session.close()
    
    def validate_and_prepare_data(self, table_name, data):
        """Validate and prepare data for insertion, handling defaults and nulls."""
        if not data:
            return []
            
        columns = self.get_table_columns(table_name)
        logging.info(f"Table schema for {table_name}:")
        for col_name, col_info in columns.items():
            logging.info(f"  {col_name}: {col_info}")
        
        prepared_data = []
        for idx, record in enumerate(data):
            prepared_record = {}
            
            # Check each column in the table schema
            for col_name, col_info in columns.items():
                # Skip if column has default value or is nullable
                if col_name in record:
                    prepared_record[col_name] = record[col_name]
                elif col_info['default'] is not None:
                    # Skip columns with defaults
                    continue
                elif col_info['nullable']:
                    prepared_record[col_name] = None
                else:
                    # Only raise error if column is NOT NULL and has no default
                    logging.error(f"Record {idx}: Missing required column '{col_name}' "
                                f"(not nullable, no default value)")
                    raise ValueError(f"Missing required column '{col_name}' in record {idx}")
            
            # Add prepared record
            prepared_data.append(prepared_record)
            
        return prepared_data


    def validate_data(self, table_name, data):
        """Validate data against table schema before insertion."""
        try:
            # Get table schema
            inspector = inspect(self.engine)
            columns = self.get_table_columns(table_name)
            
            if not columns:
                raise ValueError(f"Table '{table_name}' not found or has no columns")
            
            # Check if data matches schema
            sample_record = data[0] if data else {}
            missing_cols = set(columns.keys()) - set(sample_record.keys())
            extra_cols = set(sample_record.keys()) - set(columns.keys())
            
            if missing_cols:
                logging.error(f"Missing required columns: {missing_cols}")
            if extra_cols:
                logging.warning(f"Extra columns that will be ignored: {extra_cols}")
            
            # Log data types
            logging.debug("Column types in database:")
            for col, info in columns.items():
                logging.debug(f"{col}: {info['type']}")
            
            logging.debug("Data types in first record:")
            for col, value in sample_record.items():
                logging.debug(f"{col}: {type(value)}")
                
            return bool(not missing_cols)
            
        except Exception as e:
            logging.error(f"Error validating data: {str(e)}", exc_info=True)
            raise

    def insert_data(self, table_name, data, batch_size=5000):
        """Insert data into specified table with enhanced error handling."""
        if not data:
            logging.warning("No data provided for insertion")
            return
            
        total_records = len(data)
        logging.info(f"Attempting to insert {total_records} records into {table_name}")
        
        # Validate and prepare data
        prepared_data = self.validate_and_prepare_data(table_name, data)
        
        # Get table object
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        
        with self.session_scope() as session:
            try:
                for i in range(0, total_records, batch_size):
                    batch = prepared_data[i:i + batch_size]
                    
                    # Log sample of batch data for debugging
                    if i == 0:
                        sample_size = min(2, len(batch))
                        logging.debug(f"Sample of first {sample_size} records:")
                        for idx, record in enumerate(batch[:sample_size]):
                            logging.debug(f"Record {idx + 1}: {record}")
                    
                    try:
                        # Create the insert statement
                        stmt = insert(table).values(batch)
                        
                        # Execute the insert
                        session.execute(stmt)
                        session.flush()
                        logging.info(f"Successfully inserted batch {i//batch_size + 1} ({len(batch)} records)")
                    except Exception as e:
                        logging.error(f"Error processing batch starting at record {i}:")
                        logging.error(f"Error details: {str(e)}")
                        logging.error(f"First record in failed batch: {batch[0] if batch else 'No data'}")
                        raise
                        
            except Exception as e:
                logging.error(f"Error during batch insertion: {str(e)}")
                raise
                
        logging.info(f"Successfully inserted {total_records} records")
    
    def get_table_columns(self, table_name):
        """Get detailed column information for a specified table."""
        inspector = inspect(self.engine)
        try:
            columns = inspector.get_columns(table_name)
            if not columns:
                logging.warning(f"No columns found for table '{table_name}'")
            
            # Get primary key and foreign key info
            pk_constraint = inspector.get_pk_constraint(table_name)
            pk_columns = set(pk_constraint['constrained_columns']) if pk_constraint else set()
            
            # Enhance column information
            column_info = {}
            for col in columns:
                column_info[col['name']] = {
                    'type': col['type'],
                    'nullable': col.get('nullable', True),
                    'default': col.get('default'),
                    'is_primary_key': col['name'] in pk_columns,
                    'python_type': self._get_python_type(col['type'])
                }
            
            return column_info
        except Exception as e:
            logging.error(f"Error getting columns for table '{table_name}': {str(e)}")
            raise
            
    def print_table_schema(self, table_name):
        """Print detailed table schema for debugging."""
        try:
            columns = self.get_table_columns(table_name)
            logging.info(f"\nSchema for table '{table_name}':")
            for col_name, col_info in columns.items():
                logging.info(f"Column: {col_name}")
                logging.info(f"  Type: {col_info['type']}")
                logging.info(f"  Nullable: {col_info.get('nullable', True)}")
                if col_info.get('default'):
                    logging.info(f"  Default: {col_info['default']}")
        except Exception as e:
            logging.error(f"Error getting table schema: {str(e)}")

    def _get_python_type(self, sql_type):
        """Map SQL types to Python types."""
        type_str = str(sql_type).lower()
        if 'int' in type_str:
            return int
        elif 'float' in type_str or 'double' in type_str or 'numeric' in type_str:
            return float
        elif 'bool' in type_str:
            return bool
        elif 'timestamp' in type_str or 'datetime' in type_str:
            return datetime
        elif 'date' in type_str:
            return datetime.date
        else:
            return str



    def insert_data1(self, table_name, data, batch_size=1000):
        """Insert data into specified table with enhanced error handling and validation."""
        if not data:
            logging.warning("No data provided for insertion")
            return
            
        total_records = len(data)
        logging.info(f"Attempting to insert {total_records} records into {table_name}")
        
        # Validate data before insertion
        if not self.validate_data(table_name, data):
            raise ValueError("Data validation failed")
        
        # Get table object
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        
        with self.session_scope() as session:
            try:
                for i in range(0, total_records, batch_size):
                    batch = data[i:i + batch_size]
                    
                    # Log sample of batch data for debugging
                    if i == 0:  # Only log first batch
                        sample_size = min(2, len(batch))
                        logging.debug(f"Sample of first {sample_size} records:")
                        for idx, record in enumerate(batch[:sample_size]):
                            logging.debug(f"Record {idx + 1}: {record}")
                    
                    try:
                        session.bulk_insert_mappings(table, batch)
                        session.flush()
                        logging.info(f"Successfully inserted batch {i//batch_size + 1} ({len(batch)} records)")
                    except Exception as e:
                        logging.error(f"Error processing batch starting at record {i}:", exc_info=True)
                        logging.error(f"Error details: {str(e)}")
                        logging.error(f"First record in failed batch: {batch[0] if batch else 'No data'}")
                        raise
                        
            except Exception as e:
                logging.error(f"Error during batch insertion: {str(e)}", exc_info=True)
                raise
                
        logging.info(f"Successfully inserted {total_records} records")

    def get_table_columnsX(self, table_name):
        """Get column information for a specified table."""
        inspector = inspect(self.engine)
        try:
            columns = inspector.get_columns(table_name)
            if not columns:
                logging.warning(f"No columns found for table '{table_name}'")
            return {col['name']: col for col in columns}
        except Exception as e:
            logging.error(f"Error getting columns for table '{table_name}': {str(e)}")
            raise

    def insert_dataX(self, table_name, data, batch_size=1000):
        """Insert data into specified table with batch processing."""
        if not data:
            logging.warning("No data provided for insertion")
            return

        # Get the table metadata
        table = self.metadata.tables.get(table_name)
        if not table:
            logging.error(f"Table '{table_name}' not found in metadata")
            return

        # Retrieve the table's column information
        table_columns = self.get_table_columns(table_name)
        if not table_columns:
            logging.error(f"Cannot insert data. Table '{table_name}' does not exist or has no columns.")
            return
        
        # Verify data structure matches table columns
        first_record = data[0]
        if not all(col in table_columns for col in first_record):
            logging.error(f"Data does not match table schema for '{table_name}'. Expected columns: {list(table_columns.keys())}")
            return
        
        total_records = len(data)
        logging.info(f"Attempting to insert {total_records} records into {table_name}")
        
        with self.session_scope() as session:
            try:
                for i in range(0, total_records, batch_size):
                    batch = data[i:i + batch_size]
                    logging.debug(f"Inserting batch {i // batch_size + 1} ({len(batch)} records)")
                    logging.debug(f"Batch data sample: {batch[:2]}")  # Log a sample of the batch
                    stmt = insert(table).values(batch)
                    session.execute(stmt)
                    session.flush()  # Force batch insertion
                    logging.debug(f"Successfully inserted batch {i//batch_size + 1}")
            except Exception as e:
                logging.error(f"Unexpected error inserting data into {table_name} at batch {i // batch_size + 1}: {str(e)}")
                logging.debug(f"Failed batch data: {batch}")
                raise
        
        logging.info(f"Successfully inserted {total_records} records into {table_name}")
 
    def test_connection(self):
        """Test database connection and return connection details."""
        db_config = self.config.db_config
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT version()"))
                return {
                    'status': 'connected',
                    'host': db_config['host'],
                    'port': db_config['port'],
                    'database': db_config['database'],
                    'user': db_config['user']
                }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'host': db_config['host'],
                'port': db_config['port'],
                'database': db_config['database'],
                'user': db_config['user']
            }