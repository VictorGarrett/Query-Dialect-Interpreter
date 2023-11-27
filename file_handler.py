from table import Table
import os
import csv


def save_table_to_database(table: Table, to_database: str):

    if not os.path.exists("./" + to_database): 
        os.makedirs("./" + to_database) 

    with open("./" + to_database + "/" + table.table_name + ".banco", 'w', newline='') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=table.column_names)

        writer.writeheader()
        for row in table.data:
            writer.writerow(row)

def load_table_from_database(table_name: str, from_database: str):

    new_table = Table(table_name , [])

    file = "./" + from_database + "/" + table_name + '.banco'

    with open(file, newline='') as csv_file:
        reader = csv.DictReader(csv_file, quotechar='"', skipinitialspace=True)
        new_table.set_column_names(reader.fieldnames)

        for row in reader:
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