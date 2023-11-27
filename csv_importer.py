import csv
from table import Table
from os import listdir
import file_handler
import os
import re

def import_dir_csv_to_database(directory: str, to_database: str):

    dirs = listdir(directory) 
    print(dirs)
    print(dirs[0])
    for file in listdir(directory):
        if file[-4:] == ".csv":
            new_table = import_table_csv(file, re.split("/|\.", file)[-2], [])
            file_handler.save_table_to_database(new_table, to_database)

def import_table_csv(file: str, table_name: str, column_names: list, delimiter = ','):

    new_table = Table(table_name ,column_names)

    with open(file, newline='') as csv_file:
        reader = csv.DictReader(csv_file, quotechar='"', skipinitialspace=True)
        new_table.set_column_names(reader.fieldnames)
       #if len(column_names) == 0:
            #reader.
            #names_to_csv_col = {}
            #i=0
            #for col_name in table_names:
            #    names_to_csv_col[col_name] = i
            #    i += 1

        for row in reader:
            #new_row = {}
            #column_index = 0
            #for column in row:
            #    new_row[column_names[column_index]] = column 
            #    column_index += 1
            for key in row.keys():
                try:
                    row[key] = int(row[key])
                except ValueError:
                    try:
                        row[key] = float(row[key])
                    except ValueError:
                        continue
            new_table.add_row(row)
    
    return new_table

def export_table(file: str, table: Table):

    with open(file, 'w', newline='') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=table.column_names)

        writer.writeheader()
        for row in table.data:
            writer.writerow(row)
