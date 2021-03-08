import sys

import lab_to_db_updater
import csv_creater

"""
This program creates a master csv to combine lab results with procurement and material processing data. From the SQL database, it relies on the material_procurement, ball_milling, and hot_press tables. It handles ICP and Hall lab results files, recording measurments for the Ball Milling and Hot Press processes, by uploading this information to the SQL database and then combining it into a master csv.

Any updates or formating changes to the lab result files or additional measurements should be handled in lab_to_db_updater.py file, but will also affect the csv_creater file which only handles ICP and Hall measurements.

Changes to the SQL database, particularly column names, will affect the create_master_csv function in csv_creater file.
"""

if __name__ == '__main__':
    if len(sys.argv) == 3:
        # collect arguements provided
        lab_files_directory = sys.argv[1]
        connect_statement = sys.argv[2]
        try:
            # create new tables in database if they do not exist
            lab_to_db_updater.create_lab_tables(connect_statement)
        except Exception as e:
            print(e)
            print('Arguement 1 must be an SQL connection argument to your database.')
            sys.exit()
        try:
            # add new lab records if they do not exist
            lab_to_db_updater.add_new_lab_results(connect_statement, lab_files_directory)
        except Exception as e:
            print(e)
            print('Arguement 2 must be the directory where only lab.txt files are located.')
            sys.exit()
        # create the master csv
        csv_creater.create_master_csv(connect_statement)
    else:
        print("Please specify 2 arguements:")
        print("1. The directory in quotes where only lab.txt files are located (e.g., 'lab_files/').")
        print("2. A SQL connection argument to your database in quotes (e.g., 'dbname=citrine user=dale')")
