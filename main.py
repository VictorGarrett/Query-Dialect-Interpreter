import csv_importer
from table import join_tables
import mysql_importer
import file_handler
from query_parser import Parser


def handle_import():
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

def handle_query():
    print("Digite o banco em que deseja fazer a consulta:")
    db = input()
    print("Digite sua consulta:")
    query_input_string = input()

    parser = Parser(db)
    parser.parse_query(query_input_string)



option = None
while option != "Sair":

    print("Digite uma opção (Importar, Consualtar, Sair):")
    option = input()
    if option == "Importar":
        handle_import()
    elif option == "Consultar":
        handle_query()
    


