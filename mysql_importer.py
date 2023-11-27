import mysql.connector as connector
from table import Table
import file_handler



def import_mysql_to_database(user: str, pwd: str, mysql_database: str, tables: list, to_database: str):
    cnx = connector.connect(user=user, password=pwd,
                                    host='127.0.0.1',
                                    database=mysql_database)

    if cnx and cnx.is_connected():

        with cnx.cursor() as cursor:

            for table_name in tables:
                #cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.tables where TABLE_SCHEMA = \"employees\" AND TABLE_TYPE = \"BASE TABLE\"")
                cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.columns where TABLE_SCHEMA = \"" + mysql_database + "\" AND TABLE_NAME = \"" + table_name +"\"")

                #print("rows")
                rows = cursor.fetchall()
                #print(rows)
                #print("for")
                field_names = []
                for row in rows:
                    field_names.append(row[0])
                    print(row)

                #print(field_names)

                field_names_str = ""
                for field in field_names:
                    field_names_str = field_names_str + field + ', '

                field_names_str = field_names_str[:-2]
                #print(field_names_str)

                cursor.execute("SELECT " + field_names_str + " FROM " + table_name)

                rows = cursor.fetchall()

                new_table = Table(table_name, field_names)

                for row in rows:
                    new_row = {}
                    for i in range(len(field_names)):
                        new_row[field_names[i]] = row[i]
                    new_table.add_row(new_row)

                #print("newtable")
                #print(new_table.data)                
                #importer.export_table(table_name + ".csv", new_table)
                file_handler.save_table_to_database(new_table, to_database)

        cnx.close()

    else:

        print("Could not connect")