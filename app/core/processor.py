# app/core/processor.py
from datetime import datetime
import os
import uuid
from app.models.control import ControlTable

class FileProcessor:
    def __init__(self, config, db_handler, file_handler, data_validator, logger):
        self.config = config
        self.db_handler = db_handler
        self.file_handler = file_handler
        self.data_validator = data_validator
        self.logger = logger
    
    def process_file(self, file_path, pattern_config):
        """Process a single file according to pattern configuration"""
        process_id = str(uuid.uuid4())
        file_name = os.path.basename(file_path)
        
        # Create control record
        control_record = ControlTable(
            process_id=process_id,
            file_name=file_name,
            file_path=file_path,
            target_table=pattern_config['table'],
            status='INITIATED',
            created_by='SYSTEM',
            file_location='INPUT'
        )
        
        try:
            with self.db_handler.session_scope() as session:
                session.add(control_record)
                session.commit()
                
                # Update status to IN_PROGRESS
                control_record.status = 'IN_PROGRESS'
                session.commit()
                
                # Process the file
                self._process_file_content(file_path, pattern_config, control_record, session)
                
                # Move to archive and update status
                self._archive_file(file_path, file_name, control_record, session)
                
        except Exception as e:
            self.logger.logger.error(f"Error processing file {file_name}: {str(e)}")
            self._handle_error(file_path, file_name, control_record, str(e))
    
    def _process_file_content(self, file_path, pattern_config, control_record, session):
        """Process the content of a file"""
        file_type = os.path.splitext(file_path)[1][1:].lower()
        read_options = pattern_config.get('read_options', {}).get(file_type, {})

        self.logger.logger.debug(f"Reading file {file_path} of type {file_type} with options {read_options}")

        # Read file
        df = self.file_handler.read_file(file_path, file_type, read_options)
        self.logger.logger.debug(f"File read successfully. Total rows: {len(df)}")

        control_record.total_rows = len(df)
        session.commit()

        # Validate data
        validation_results = self.data_validator.validate_data(
            df, 
            pattern_config['table']
        )
        
        if not validation_results['is_valid']:
            raise ValueError(f"Data validation failed: {validation_results['errors']}")

        # Add metadata columns
        self._add_metadata_columns(df, control_record.process_id)

        # Process in batches
        self.logger.logger.debug(f"Processing file content in batches for table {pattern_config['table']}")
        self._process_batches(df, pattern_config['table'], control_record, session)
    
    def _add_metadata_columns(self, df, process_id):
        """Add metadata columns to DataFrame"""
        # metadata_columns = {
        #     'created_date': func.now(),
        #     'created_by': 'SYSTEM',
        #     'modified_date': func.now(),
        #     'modified_by': 'SYSTEM',
        #     'load_batch_no': process_id
        # }

        metadata_columns = {
            'load_batch_no': process_id
        }

        for col, value in metadata_columns.items():
            if col not in df.columns:
                df[col] = value
    
    def _process_batches(self, df, table_name, control_record, session):
        """Process DataFrame in batches"""
        batch_size = 1000
        total_loaded = 0

        self.logger.logger.debug(f"Starting batch processing for {len(df)} records")

        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            self.logger.logger.debug(f"Processing batch {i // batch_size + 1}")

            try:
                control_record.current_batch = i // batch_size + 1
                session.commit()

                records = batch_df.to_dict('records')
                self.logger.logger.debug(f"Inserting batch {i // batch_size + 1} into {table_name}")
                
                self.db_handler.insert_data(table_name, records, batch_size)
                
                total_loaded += len(batch_df)
                control_record.loaded_rows = total_loaded
                session.commit()

                self.logger.logger.debug(f"Successfully inserted batch {i // batch_size + 1}")
                
            except Exception as e:
                self.logger.logger.error(f"Error processing batch {i // batch_size + 1}: {str(e)}")
                raise Exception(f"Error processing batch {i // batch_size + 1}: {str(e)}")

        self.logger.logger.debug(f"Finished processing batches, total records loaded: {total_loaded}")
    
    def _archive_file(self, file_path, file_name, control_record, session):
        """Move file to archive folder and update status"""
        archive_path = os.path.join(
            self.config.storage_config['archive_folder'],
            f"{datetime.now().strftime('%Y%m%d')}_{file_name}"
        )
        self.file_handler.move_file(file_path, archive_path)
        
        control_record.status = 'SUCCESS'
        control_record.file_location = 'ARCHIVE'
        session.commit()
        
        self.logger.logger.info(
            f"Successfully processed file {file_name}. "
            f"Loaded {control_record.loaded_rows} records."
        )
    
    def _handle_error(self, file_path, file_name, control_record, error_message):
        """Handle file processing error"""
        try:
            with self.db_handler.session_scope() as session:
                control_record.status = 'ERROR'
                control_record.error_message = error_message
                
                error_path = os.path.join(
                    self.config.storage_config['error_folder'],
                    f"{datetime.now().strftime('%Y%m%d')}_{file_name}"
                )
                self.file_handler.move_file(file_path, error_path)
                control_record.file_location = 'ERROR'
                session.commit()
                
        except Exception as move_error:
            self.logger.logger.error(
                f"Error moving file to error folder: {str(move_error)}"
            )

    def retry_failed_files(self, start_from_failure=True):
        """Retry processing failed files"""
        with self.db_handler.session_scope() as session:
            failed_processes = session.query(ControlTable).filter(
                ControlTable.status == 'ERROR'
            ).all()
            
            for process in failed_processes:
                self._retry_file(process, start_from_failure, session)
    
    def _retry_file(self, process, start_from_failure, session):
        """Retry processing a single failed file"""
        if start_from_failure:
            last_batch = process.current_batch or 0
            self.logger.logger.info(
                f"Retrying file {process.file_name} from batch {last_batch + 1}"
            )
        else:
            process.current_batch = 0
            process.loaded_rows = 0
            self.logger.logger.info(
                f"Reprocessing entire file {process.file_name}"
            )
        
        source_path = os.path.join(
            self.config.storage_config['error_folder'],
            process.file_name
        )
        dest_path = os.path.join(
            self.config.storage_config['input_folder'],
            process.file_name
        )
        
        try:
            self.file_handler.move_file(source_path, dest_path)
            process.file_location = 'INPUT'
            process.status = 'PENDING_RETRY'
            session.commit()
        except Exception as e:
            self.logger.logger.error(
                f"Error moving file back to input folder: {str(e)}"
            )