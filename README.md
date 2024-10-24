# ATHENA

Greek goddess of wisdom, war and handicraft.

Key engine part of Project Hermes - supports data load from flat files into database tables

***************************************

## Athena Database

### Database Setup
````
create database athena_db;
alter user athena with encrypted password '***';
grant all privileges on database athena_db to athena;
ALTER DATABASE athena_db OWNER TO athena;
GRANT ALL ON SCHEMA public TO athena;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
SELECT rolname FROM pg_roles where rolname like 'athena%';
````

### File Processing Controller
````
CREATE TABLE public.file_processing_control (
	id serial4 NOT NULL,
	created_date timestamp NOT NULL,
	created_by varchar(100) NOT NULL,
	modified_date timestamp NULL,
	modified_by varchar(100) NULL,
	process_id uuid NOT NULL,
	file_id uuid NULL,
	file_name varchar(255) NOT NULL,
	file_path varchar(500) NOT NULL,
	target_table varchar(100) NOT NULL,
	status varchar(50) NOT NULL,
	total_rows int4 NULL,
	loaded_rows int4 NULL,
	error_message text NULL,
	current_batch int4 NULL,
	file_location varchar(1000) null,
	archive_location varchar(1000) NULL,
	CONSTRAINT file_processing_control_pkey PRIMARY KEY (id)
);
````

## Trade Database - US Manifest Data

### Consignee 
````
CREATE TABLE ams_consignee_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	consignee_revision varchar(10),
	identifier varchar(30),
	consignee_name varchar(4000),
	consignee_address_1 varchar(4000),
	consignee_address_2 varchar(4000),
	consignee_address_3 varchar(4000),
	consignee_address_4 varchar(4000),
	city varchar(1000),
	state_province varchar(1000),
	zip_code varchar(100),
	country_code varchar(100),
	contact_name varchar(1000),
	comm_number_qualifier varchar(1000),
	comm_number  varchar(1000),
	CONSTRAINT ams_consignee_load_pk PRIMARY KEY (id)
);
````

### Bill Gen
````
CREATE TABLE ams_billgen_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	billgen_revision varchar(10),
	identifier  varchar(30),
	master_bol_number varchar(500),
	house_bol_number varchar(500),
	sub_house_bol_number varchar(500),
	voyage_number varchar(500),
	bill_type_code varchar(500),
	manifest_number varchar(1000),
	trade_update_date varchar(50),
	run_date  varchar(50),
	CONSTRAINT ams_billgen_load_pk PRIMARY KEY (id)
);
````

### Marks Numbers
````
CREATE TABLE ams_marksnumbers_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	marksnumbers_revision varchar(10),	
	identifier varchar(30),
	container_number varchar(30),
	marks_and_numbers_1 varchar(100),
	marks_and_numbers_2 varchar(100),
	marks_and_numbers_3 varchar(100),
	marks_and_numbers_4 varchar(100),
	marks_and_numbers_5 varchar(100),
	marks_and_numbers_6 varchar(100),
	marks_and_numbers_7 varchar(100),
	marks_and_numbers_8 varchar(100),
	CONSTRAINT ams_marksnumbers_load_pk PRIMARY KEY (id)
);	
````

### Header
````
CREATE TABLE ams_header_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	header_revision varchar(10),
    identifier varchar(30),
    carrier_code varchar(100),
    vessel_country_code varchar(100),
    vessel_name varchar(500),
    port_of_unlading varchar(500),
    estimated_arrival_date varchar(50),
    foreign_port_of_lading_qualifier varchar(100),
    foreign_port_of_lading varchar(500),
    manifest_quantity varchar(30),
    manifest_unit varchar(30),
    weight varchar(30),
    weight_unit varchar(30),
    measurement varchar(30),
    measurement_unit varchar(30),
    record_status_indicator varchar(30),
    place_of_receipt varchar(500),
    port_of_destination varchar(500),
    foreign_port_of_destination_qualifier varchar(100),
    foreign_port_of_destination varchar(500),
    conveyance_id_qualifier varchar(100),
    conveyance_id varchar(100),
    in_bond_entry_type varchar(100),
    mode_of_transportation varchar(100),
    secondary_notify_party_1 varchar(1000),
    secondary_notify_party_2 varchar(1000),
    secondary_notify_party_3 varchar(1000),
    secondary_notify_party_4 varchar(1000),
    secondary_notify_party_5 varchar(1000),
    secondary_notify_party_6 varchar(1000),
    secondary_notify_party_7 varchar(1000),
    secondary_notify_party_8 varchar(1000),
    secondary_notify_party_9 varchar(1000),
    secondary_notify_party_10 varchar(1000),
    actual_arrival_date varchar(50),
	CONSTRAINT ams_header_load_pk PRIMARY KEY (id)
);	
````

### Cargo Desc
````
CREATE TABLE ams_cargodesc_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	cargodesc_revision varchar(10),
	identifier varchar(30),
	container_number varchar(30),
	description_sequence_number varchar(30),
	piece_count varchar(30),
	description_text varchar(5000),
	CONSTRAINT ams_cargodesc_load_pk PRIMARY KEY (id)
);	
````

### Tariff
````
CREATE TABLE ams_tariff_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	tariff_revision varchar(10),	
	identifier varchar(30),
	container_number varchar(30),
	description_sequence_number varchar(30),
	harmonized_number varchar(30),
	harmonized_value varchar(30),
	harmonized_weight varchar(30),
	harmonized_weight_unit varchar(30),	
	CONSTRAINT ams_tariff_load_pk PRIMARY KEY (id)
);	
````

### Notify Party
````
CREATE TABLE ams_notifyparty_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	notifyparty_revision varchar(10),	
	identifier varchar(30),
	notify_party_name varchar(4000),
	notify_party_address_1 varchar(4000),
	notify_party_address_2 varchar(4000),
	notify_party_address_3 varchar(4000),
	notify_party_address_4 varchar(4000),
	city varchar(1000),
	state_province varchar(1000),
	zip_code varchar(100),
	country_code varchar(100),
	contact_name varchar(1000),
	comm_number_qualifier varchar(1000),
	comm_number varchar(1000),
	CONSTRAINT ams_notifyparty_load_pk PRIMARY KEY (id)
);	
````

### Container
````
CREATE TABLE ams_container_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	container_revision varchar(10),
	identifier varchar(30),
	container_number varchar(30),
	seal_number_1 varchar(100),
	seal_number_2 varchar(100),
	equipment_description_code varchar(100),
	container_length varchar(30),
	container_height varchar(30),
	container_width varchar(30),
	container_type varchar(100),
	load_status varchar(30),
	type_of_service varchar(100),
	CONSTRAINT ams_container_load_pk PRIMARY KEY (id)
);	
````

### Shipper
````
CREATE TABLE ams_shipper_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	shipper_revision varchar(10),	
	identifier varchar(30),
	shipper_party_name varchar(4000),
	shipper_party_address_1 varchar(4000),
	shipper_party_address_2 varchar(4000),
	shipper_party_address_3 varchar(4000),
	shipper_party_address_4 varchar(4000),
	city varchar(100),
	state_province varchar(100),
	zip_code varchar(100),
	country_code varchar(100),
	contact_name varchar(1000),
	comm_number_qualifier varchar(1000),
	comm_number  varchar(1000),
	CONSTRAINT ams_shipper_load_pk PRIMARY KEY (id)
);	
````

### Hazmat Class
````
CREATE TABLE ams_hazmatclass_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	hazmatclass_revision varchar(10),	
	identifier varchar(30),
	container_number varchar(30),
	hazmat_sequence_number varchar(100),
	hazmat_classification varchar(5000),
	CONSTRAINT ams_hazmatclass_load_pk PRIMARY KEY (id)
);
````

### Hazmat
````
CREATE TABLE ams_hazmat_load (
	id UUID NOT NULL DEFAULT uuid_generate_v4() , 
	created_by varchar(75)  NULL,
	created_date timestamp default current_timestamp not null,
	modified_by varchar(75)  NULL,
	modified_date timestamp NULL,
	load_batch_no UUID,
	data_status smallint default 0,
	hazmat_revision varchar(10),	
	identifier varchar(30),
	container_number varchar(30),
	hazmat_sequence_number varchar(30),
	hazmat_code varchar(30),
	hazmat_class varchar(30),
	hazmat_code_qualifier varchar(100),
	hazmat_contact varchar(1000),
	hazmat_page_number varchar(30),
	hazmat_flash_point_temperature varchar(30),
	hazmat_flash_point_temperature_negative_ind varchar(30),
	hazmat_flash_point_temperature_unit varchar(30),
	hazmat_description varchar(5000),
	CONSTRAINT ams_hazmat_load_pk PRIMARY KEY (id)
);
````

### Verification
````
select * from file_processing_control;
select count(*) from ams_consignee_load;
select count(*) from ams_hazmat_load;
select count(*) from ams_hazmatclass_load;
select count(*) from ams_billgen_load;
select count(*) from ams_marksnumbers_load;
select count(*) from ams_header_load;
select count(*) from ams_cargodesc_load;
select count(*) from ams_tariff_load;
select count(*) from ams_notifyparty_load;
select count(*) from ams_container_load;
select count(*) from ams_shipper_load;
````