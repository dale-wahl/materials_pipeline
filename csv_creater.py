import pandas as pd
import datetime

import postgres_handler
from lab_to_db_updater import measurement_types

sql_materials_table_name = "material_procurement"
sql_ball_milling_table_name = "ball_milling"
sql_hot_press_table_name = "hot_press"

def create_master_csv(connect_statement):
    """
    This creates a master csv file by pulling data from the database and the recently loaded lab result files. It assumes that uid is unique in each table.

    It is designed with a one to one relationship in mind for ball milling and hot press, however it will still attempt to merge if that is not the case.

    It does assumes that only one ICP and one Hall measurement can be collected for each ball milling output and each hot press process (i.e., there can be a maximum of four measurements for any given matterial procured).

    The likelist point of failure would be any changes to the database such as names of tables or names of columns.

    Any updates or additional lab result measurment files will have to be addressed both here and in the lab_to_db_updater.py file.
    """
    # Connect to SQL database
    sql = postgres_handler.SQL_Connector(connect_statement)

    # Collect existing tables
    material_procurement = sql.collect_all_table_records(sql_materials_table_name)
    material_procurement = pd.DataFrame(material_procurement[0], columns=material_procurement[1])
    ball_milling = sql.collect_all_table_records(sql_ball_milling_table_name)
    ball_milling = pd.DataFrame(ball_milling[0], columns=ball_milling[1])
    hot_press = sql.collect_all_table_records(sql_hot_press_table_name)
    hot_press = pd.DataFrame(hot_press[0], columns=hot_press[1])

    # Rename columns with names in multiple tables
    ball_milling.rename(columns={'process_name' : 'ball_milling_process_name',
                             'output_material_name' : 'ball_milling_output_material_name',
                             'output_material_uid': 'ball_milling_output_material_uid'}, inplace=True)
    hot_press.rename(columns={'process_name' : 'hot_press_process_name',
                             'output_material_name' : 'hot_press_output_material_name',
                             'output_material_uid': 'hot_press_output_material_uid'}, inplace=True)

    # Merge ball milling and hot press tables
    sql_df = ball_milling.merge(hot_press,
                                how='outer',
                                left_on='hot_press_uid',
                                right_on='uid',
                                suffixes=('_ball_milling', '_hot_press'))

    sql_df = split_and_merge_materials(merge_df=sql_df,
                                       materials_df=material_procurement,
                                       merge_df_column_to_merge_on='uid_ball_milling',
                                       materials_df_column_to_merge_on='ball_milling_uid',
                                       material_type_column_name='material_name')

    # Rename and drop duplicative columns
    try:
        sql_df.rename(columns={'uid_ball_milling' : 'ball_milling_uid'}, inplace=True)
        sql_df = sql_df.drop(['uid_hot_press'], axis=1)
    except:
        # Prevents failure due to column names not existing; code will work, but nameing my be confusing
        pass

    # List of processes to search for lab results for their output materials
    # [(processes_abreviation, process_output_material_uid_column_name)]
    processes = [('bm', 'ball_milling_output_material_uid'),
                ('hp', 'hot_press_output_material_uid')]

    # Loop through measurements (as defined in lab_to_db_updater file) and add them to the csv
    for measurement in measurement_types:
        sql_df = add_lab_results(sql, sql_df, measurement, processes)

    # Collect today's date
    now = datetime.datetime.now(datetime.timezone.utc)
    today = str(now.year) + '-' + str(now.month) + '-' + str(now.day)

    # Create csv file
    name_of_output = 'master_' + today + '.csv'
    sql_df.to_csv(name_of_output)
    print('Created new master csv:', name_of_output)

def split_and_merge_materials(merge_df, materials_df, merge_df_column_to_merge_on, materials_df_column_to_merge_on, material_type_column_name):
    """
    Helper function to dynamically create columns based on however many materials are used in processes.

    """
    for material in materials_df[material_type_column_name].unique():
        temp_df = materials_df[materials_df[material_type_column_name] == material]
        temp_df.columns = [str(material) + '_' + column for column in temp_df.columns]
        merge_df = merge_df.merge(temp_df,
                                  how='left',
                                  left_on=merge_df_column_to_merge_on,
                                  right_on=material + '_' + materials_df_column_to_merge_on)
        merge_df.drop(material + '_' + materials_df_column_to_merge_on, axis=1, inplace=True)
    return merge_df

def add_lab_results(sql, merge_df, measurement, processes):
    """
    Helper function to allow additional lab results measurement types to be added to master.

    merge_df = dataframe where lab results should be merged
    measurement = dictionary object containing measurment information
                  {'sql_table_name' : sql_table_name , 'sql_unique_id_column_name' : sql_unique_id_column_name}
    processes = list of processes to check for lab results
                [(processes_abreviation, process_output_material_uid_column_name)]
    """
    # Collect new lab result tables
    lab_table = sql.collect_all_table_records(measurement['sql_table_name'])
    lab_table = pd.DataFrame(lab_table[0], columns=lab_table[1])

    # Loop through provided processes to search for matching lab results
    for process in processes:
        # Create naming for columns and mergeing
        prefix = process[0]+'_'+measurement['sql_table_name']+'_'
        new_unique_column = prefix + measurement['sql_unique_id_column_name']

        # Merge lab results by process_output_material_uid_column_name
        merge_df = merge_df.merge(lab_table.add_prefix(prefix),
                                    how='left',
                                    left_on=process[1],
                                    right_on=new_unique_column)

        # Create some columns for easy identifying where measurments exist and drop duplicate column
        merge_df[prefix + 'results'] = merge_df[new_unique_column].notnull()
        merge_df.drop(new_unique_column, axis=1, inplace=True)
    return merge_df
