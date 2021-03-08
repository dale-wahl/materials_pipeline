# Lab Loader and Master CSV Creater

## Intro
This python program is designed to connect to a postgres SQL database and a directory containing lab result text documents. It will then upload the lab results into the database and output a master csv file in the following format:

Ball Milling Process | Hot Press Process | Material 1 | Material 2 | Ball Mill Lab 1 | Ball Mill Lab 2 | Hot Press Lab 1 | Hot Press Lab 2
 ------------------- | ----------------- | ---------- | ---------- | --------------- | --------------- | --------------- | ---------------

## Setup
This program makes use of the pandas and psycopg2 Python libraries.
You can use the requirements.txt to create the appropriate Python 3 environment with the following packages:
* numpy==1.20.1
* pandas==1.2.3
* psycopg2==2.8.6
* python-dateutil==2.8.1
* pytz==2021.1
* six==1.15.0

1. Run the processdb.sql file to create a copy of up the basic database (you will need postgres or similar to run the database)
2. Create python3 environment

## Running program
The collect_labs.py file is the main application file since the program is designed to be run whenever new lab result files are created.

Two arguments are required:
1. The directory in quotes where only lab.txt files are located (e.g., 'lab_files/')
2. A SQL connection argument to the database in quotes (e.g., 'dbname=citrine user=dale')

For example:
```
python collect_labs.py 'x-lab-data/' 'dbname=citrine user=dale'
```

## Modifications
### Measurements
The basic design of this program is to collect lab measurements from .txt files. New types of measurements or changes to existing measurements should be addressed in the lab_to_db_updater.py file. It assumes that the "Measurement" field in the lab result text file can be used to differentiate the types of measurements. Additional measurements will be added to the master csv. Currently the program handles ICP and Hall measurements.

Changes to the format of the lab result files can be addressed in the lab_file_handler.py file. The handler does assume all files will be .txt files. This can be modified here as well.

### Materials
Materials are derived from the material_procurement table in the PostgresDB. The program should dynamically handles new materials if they are added. Materials appear only to be connected to the ball milling process.

### Processes
This part of the program is the least dynamic. It assumes a one to one relationship between the ball milling process and the hot press process. Measurements are compared to both processes and additional processes could be added, however, the csv_creater.py would need to be modified to properly connect additional processes for the master csv.

## Note on psycopg2
psycopg2 has specific handling to prevent SQL injections. Be careful making any changes to postgres_handler.py and do not use Python string concatenation (+) or string parameters interpolation (%) to pass variables to a SQL query string.
More information can be found here: https://www.psycopg.org/docs/usage.html
