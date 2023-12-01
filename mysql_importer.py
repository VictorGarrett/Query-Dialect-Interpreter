import mysql.connector as connector
from table import Table
import file_handler

user_global = 'root'
password_global = 'NuncaFoiMole.123'
database_global = 'employees'
host_global = '127.0.0.1'
destination = 'CSV_Database'


def mysql_connector():

    cnx_params = {
        'host': host_global,
        'user': user_global,
        'password': password_global,
        'database': selected_database_global,
    }

    try:
        db_connection = connector.connect(**cnx_params)
    except:
        print("error : Schema not found")
        return False

    print('\nConectado!\n')
    return db_connection


def list_mysql_schemas():
    cnx = connector.connect(user=user_global, password=password_global,
                            host=host_global, database=database_global)
    cursor = cnx.cursor()
    cursor.execute("SHOW DATABASES;")
    print("\nEsquemas no servidor MySQL: ")
    for (databases) in cursor:
        print('* '+databases[0])


def list_mysql_tables(cursor):
    print("Tabelas do esquema {}:".format(selected_database_global))
    cursor.execute("SHOW TABLES;")
    for row in cursor:
        for key in row:
            print('* ' + row[key] .strip("'"))


def importer():

    global selected_database_global

    list_mysql_schemas()
    cnx = None
    while not cnx:
        print("Digite nome do esquema a selecionar: ")
        selected_database_global = input('>> ')
        cnx = mysql_connector()

    cursor = cnx.cursor(dictionary=True, buffered=True)

    list_mysql_tables(cursor)
    print("Digite nome da(s) tabela(s) a selecionar (Utilizar notação ): ")
    tables = input('>> ')

    cnx = mysql_connector()

    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            for table_name in tables:
                # cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables where TABLE_SCHEMA = \"employees\" AND TABLE_TYPE = \"BASE TABLE\"")
                cursor.execute(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns where TABLE_SCHEMA = \"" + database_global + "\" AND TABLE_NAME = \"" + table_name +"\"")

                print("rows")
                rows = cursor.fetchall()
                print(rows)
                print("for")
                field_names = []
                for row in rows:
                    field_names.append(row[0])
                    print(row)

                print(field_names)

                field_names_str = ""
                for field in field_names:
                    field_names_str = field_names_str + field + ', '

                field_names_str = field_names_str[:-2]
                print(field_names_str)

                cursor.execute("SELECT " + field_names_str +
                               " FROM " + table_name)

                rows = cursor.fetchall()

                new_table = Table(table_name, field_names)

                for row in rows:
                    new_row = {}
                    for i in range(len(field_names)):
                        new_row[field_names[i]] = row[i]
                    new_table.add_row(new_row)

                print("newtable")
                print(new_table.data)
                # importer.export_table(table_name + ".csv", new_table)
                file_handler.save_table_to_database(new_table, destination)

        cnx.close()

    else:

        print("Could not connect")
