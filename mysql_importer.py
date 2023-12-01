import mysql.connector as connector
from table import Table
import file_handler


def list_mysql_schemas(cursor):
    schemas = []
    cursor.execute("SHOW DATABASES;")
    print("\nEsquemas no servidor MySQL: ")
    rows = cursor.fetchall()
    for row in rows:
        print('* '+row['Database'])
        schemas.append(row['Database'])

    return schemas


def list_mysql_tables(cursor, database):
    tables = []
    print(f"Tabelas do esquema {database}:")
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables where TABLE_SCHEMA = \"" + database + "\" and TABLE_TYPE = \"BASE TABLE\"")
    for row in cursor.fetchall():
        #print(row)
        print('* ' + row["TABLE_NAME"])
        tables.append(row["TABLE_NAME"])
    return tables

def importer():

    global selected_database_global

    print("Digite seu usuário MySQL: ")
    user = input('>> ')
    print("Digite sua senha: ")
    pwd = input('>> ')

    cnx = connector.connect(user=user, password=pwd,
                            host='127.0.0.1')
    
    if not cnx or not cnx.is_connected():
        print("could not connect")
        return
    
    cursor = cnx.cursor(dictionary=True, buffered=True)

    all_schemas = list_mysql_schemas(cursor)

    print("Digite nome do esquema a selecionar: ")
    schema = input('>> ')

    if not schema in all_schemas:
        print("esquema não reconhecido!")
        return

    all_tables = list_mysql_tables(cursor, schema)

    print("Digite nome das tabelas a selecionar (tabela1, tabela2, ...): ")
    tables_input = input('>> ')
    
    tables_input_list = tables_input.split(", ")
    #print(tables_input_list)
    tables = []
    for table_input in tables_input_list:
        if not table_input in all_tables:
            print("A tabela "+ table_input + " não existe. Ignorando")
            continue
        tables.append(table_input)
    
    print("Digite nome do banco de dados de destino: ")
    destination = input('>> ')
    

    for table_name in tables:
        # cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables where TABLE_SCHEMA = \"employees\" AND TABLE_TYPE = \"BASE TABLE\"")
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns where TABLE_SCHEMA = \"" + schema + "\" AND TABLE_NAME = \"" + table_name +"\"")

        #print("rows")
        rows = cursor.fetchall()
        #print(rows)
        #print("for")
        field_names = []
        for row in rows:
            #field_names.append(row[0])
            field_names.append(row["COLUMN_NAME"])
            #print(row)

        #print(field_names)

        field_names_str = ""
        for field in field_names:
            field_names_str = field_names_str + field + ', '

        field_names_str = field_names_str[:-2]
        #print(field_names_str)

        cursor.execute("USE " + schema)
        cursor.execute("SELECT " + field_names_str +
                        " FROM " + table_name)

        rows = cursor.fetchall()

        new_table = Table(table_name, field_names)

        for row in rows:
            #print(row)
            new_row = {}
            for i in range(len(field_names)):
                #new_row[field_names[i]] = row[i]
                new_row[field_names[i]] = row[field_names[i]]
            new_table.add_row(new_row)

        #print("newtable")
        #print(new_table.data)
        # importer.export_table(table_name + ".csv", new_table)
        file_handler.save_table_to_database(new_table, destination)

    cnx.close()
    print("importacao completa")
