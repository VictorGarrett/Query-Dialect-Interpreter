import csv_importer
from table import join_tables
import mysql_importer
import file_handler


def handle_import():
    option = None
    while not (option == "mysql" or option == "csv"):
        print("Digite uma opção de importação(mysql, csv):")
        option = input()
    if option == "mysql":
        mysql_importer.import_form_mysql()
    elif option == "csv":
        print("Digite o nome do diretório:")
        dir = input()
        print("Digite o nome do banco para o qual importar:")
        db = input()
        csv_importer.import_dir_csv_to_database(dir, db)
    return

option = None
while option != "Sair":

    print("Digite uma opção (Importar, Consualtar, Sair):")
    option = input()
    if option == "Importar":
        handle_import()
    elif option == "Consultar":
        handle_query()
    


