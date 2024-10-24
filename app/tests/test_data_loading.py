# test_data_loading.py
import logging
from app.config.config import Config
from app.core.db_handler import DatabaseHandler

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_insert():
    try:
        # Initialize
        config = Config()
        db = DatabaseHandler(config)
        
        # Your table name
        # Print table schema
        table_name = "ams_consignee_load"  # Replace with your table name
        db.print_table_schema(table_name)
        
        # Get and print table schema
        columns = db.get_table_columns(table_name)
        logging.info("\nTable schema:")
        for col_name, col_info in columns.items():
            logging.info(f"{col_name}:")
            logging.info(f"  Type: {col_info['type']}")
            logging.info(f"  Nullable: {col_info['nullable']}")
            logging.info(f"  Default: {col_info['default']}")
            logging.info(f"  Is Primary Key: {col_info['is_primary_key']}")
        
        # Load your data (example)
        data = [
                {'identifier': '201801011259', 'consignee_name': 'HONOUR LANE LOGISTICS USA INC', 'consignee_address_1': '17870 CASTLETON STREET, SUITE 270,', 'consignee_address_2': 'CITY OF INDUSTRY, CA 91748, USA', 'consignee_address_3': 'E-MAIL IMPLAX@HLSHOLDING.COM', 'consignee_address_4': '', 'city': '', 'state_province': '', 'zip_code': '', 'country_code': '', 'contact_name': '', 'comm_number_qualifier': 'Telephone Number', 'comm_number': 'TEL 1-626-3634475', 'created_by': 'SYSTEM', 'load_batch_no': '690730b1-e033-4aba-b975-3d36ccad7e8d'},
                {'identifier': '201801011259', 'consignee_name': 'HONOUR LANE LOGISTICS USA INC', 'consignee_address_1': '17870 CASTLETON STREET, SUITE 270,', 'consignee_address_2': 'CITY OF INDUSTRY, CA 91748, USA', 'consignee_address_3': 'E-MAIL IMPLAX@HLSHOLDING.COM', 'consignee_address_4': '', 'city': '', 'state_province': '', 'zip_code': '', 'country_code': '', 'contact_name': '', 'comm_number_qualifier': 'Telephone Number', 'comm_number': 'TEL 1-626-3634475', 'created_by': 'SYSTEM', 'load_batch_no': 'b643dc93-5af6-4c36-beb9-8ae349f72dad'}          
        ]
        
        # Try inserting
        db.insert_data(table_name, data)
        
    except Exception as e:
        logging.error("Error:", exc_info=True)

if __name__ == "__main__":
    test_insert()



