import csv_importer
from table import join_tables
from mysql_importer import import_mysql_to_database
import file_handler

import_mysql_to_database("root", "pwd", "employees", ["departments", "employees"], "test_emp")
csv_importer.import_dir_csv_to_database(".", "test_csv")


