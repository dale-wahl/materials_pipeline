import postgres_handler
import lab_file_handler
"""
This file uses the lab_file_handler and converts that data so that it can be uploaded into the SQL database.

The definition add_new_lab_results relies on the hardcoded values in "measurment_types" as well as the name of the "unique_id_column_name" used in the lab results file. There appear to be a one to one relationship between measurements and materials so 'material_id' acts as the unique ID; each material is only measured once.

The definition create_lab_tables, however, requires some thought in renaming whatever fields are in the lab result files into proper SQL columns and data types. This is the most manual part of the program and would need to be changed if the lab result file changes.
"""

"""
Measurements:
identifier = the text string found on the lab result file denoting the type of measurment (e.g., 'ICP', 'Hall')
sql_table_name = the name you wish the sql table to be called upon creation
sql_column_string = a sql string used to create the sql table consisting of column names and data types

measurement_types should be in the format:
{'identifier' : identifier,
'sql_table_name' : sql_table_name,
'sql_unique_id_column_name' : 'material_uid'
'sql_column_string' : sql_column_string}

NOTE: the sql_unique_id_column_name should be identical to the name used in the sql_column_string!
"""

measurement_types = [{
                      'identifier' : 'ICP',
                      'sql_table_name' : 'icp_lab',
                      'sql_unique_id_column_name' : 'material_uid',
                      'sql_column_string' : """
                      material_uid character varying(30) NOT NULL,
                      measurement character varying(10),
                      pb_concentration real,
                      sn_concentration real,
                      o_concentration real,
                      gas_flow_rate_l_min real,
                      gas_type character varying(10),
                      plasma_temperature_celsius real,
                      detector_temperature_celsius real,
                      field_strength_t real,
                      plasma_observation character varying(40),
                      radio_requency_mhz real
                      """
                      },
                      {
                       'identifier' : 'Hall',
                       'sql_table_name' : 'hall_lab',
                       'sql_unique_id_column_name' : 'material_uid',
                       'sql_column_string' : """
                       material_uid character varying(30) NOT NULL,
                       measurement character varying(10),
                       probe_resistance_ohm real,
                       gas_flow_rate_l_min real,
                       gas_type character varying(10),
                       probe_material character varying(30),
                       current_mA real,
                       field_strength_t real,
                       sample_position real,
                       magnet_reversal bool
                       """
                       }]

# This is the column name from the LAB FILE specifically used to identify if a record exists in the database or not
unique_id_column_name = 'material_uid'

class Measurement():
    def __init__(self, identifier, sql_table_name):
        """
        'id' is the identifier used in the lab result files to denote what type of measurement is used. It is case sensetive.
        'table_name' is the name used to create the SQL table for that type of measurment (per create_lab_tables).
        """
        self.id = identifier
        self.table_name = sql_table_name

        self.record_column_decode_dict = {}
        self.sql_columns = None

    def collect_sql_column_names(self, postgres_handler_sql_object):
        """
        Collect the column names in the SQL database (per create_lab_tables).
        """
        self.sql_columns = postgres_handler_sql_object.collect_table_column_names(self.table_name)

    def create_record_column_decode_dict(self, example_file, postgres_handler_sql_object):
        """
        Create a dictionary that can cross reference the field names in the lab result files with the SQL column names.
        """
        if self.record_column_decode_dict:
            pass
        else:
            raw_column_names = list(example_file.keys())
            if self.sql_columns:
                pass
            else:
                self.collect_sql_column_names(postgres_handler_sql_object)
            for name in self.sql_columns:
                self.record_column_decode_dict[raw_column_names.pop(0)] = name

def add_new_lab_results(connect_statement, lab_files_directory):
    """
    Adds all records for all new files to database from the lab_files_directory. Checks to see if record already exists based on unique_id_column_name (e.g., 'material_uid').

    Note: this creates a dictionary (record_column_decode_dict) to map the keys from the actual files to the column names that were created in the create_lab_tables function. If the lab files are updated or change and sql table has not been modified to accomidate, this will fail. If there are multiple types of lab files, they will have to be handled seperately just as ICP and Hall are currently handled seperately.
    """
    # Use lab_handler to collect lab files as dictionaries
    lab_handler = lab_file_handler.Lab_File_Handler(lab_files_directory)
    lab_handler.collect_lab_files()

    # connect to sql database
    sql = postgres_handler.SQL_Connector(connect_statement)

    # Loop through hardcoded measurements to create handler objects
    measurements = [Measurement(measurement_type['identifier'], measurement_type['sql_table_name']) for measurement_type in measurement_types]

    # count records added to database
    count = 0

    # loop through files
    for file in lab_handler.files:
        # Loop through measurements
        for measurement in measurements:
            # Check if correct measurement
            if measurement.id == file['Measurement']:
                # Create the record_column_decode_dict
                measurement.create_record_column_decode_dict(file, sql)
                # Check if record already exists
                if sql.check_record_exists(measurement.table_name,
                                        measurement.record_column_decode_dict[unique_id_column_name],
                                        file[unique_id_column_name]):
                    # record exists; do not update
                    pass
                else:
                    # Add new record
                    sql.add_record(measurement.table_name, file, measurement.record_column_decode_dict)
                    count += 1
                break
            else:
                # Not correct measurement
                pass

    sql.commit()
    sql.disconnect()

    print(count, 'records added to database.')

def create_lab_tables(connect_statement):
    """
    Creates tables hall_lab and icp_lab tables in sql database located at connect_statement. This does check to see if table already exists and, if so, does nothing.

    If additional labs are done or labs files are modified to contain additional information, the could be modified to adjust the database tables appropriately.
    """

    # Connect to database
    sql = postgres_handler.SQL_Connector(connect_statement)

    # Create sql tables
    for measurement in measurement_types:
        sql.create_table(measurement['sql_table_name'], measurement['sql_column_string'])

    # Commit and disconnect
    sql.commit()
    sql.disconnect()
