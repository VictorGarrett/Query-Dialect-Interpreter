import csv_importer
from table import join_tables
import mysql_importer as mysql_imp
import file_handler


# import_mysql_to_database("root", "NuncaFoiMole.123",
#                           "employees", ["departments", "employees"], "test_emp")
# csv_importer.import_dir_csv_to_database(".", "test_csv")


# treat the command spliting the statements
def parser(query: str):
    global commands

    commands = {
        "select": None,
        "update": None,
        "set": None,
        "insert": None,
        "delete": None,
        "into": None,
        "values": None,
        "from": None,
        "join": None,
        "on": None,
        "using": None,
        "where": None,
        "and": None,
        "or": None,
        "order by": None,
    }
    try:
        query = query.replace(", ", ",")
        query = query.replace(" ,", ",")
        query = query.replace(" =", "=")
        query = query.replace("= ", "=")
        query = query.replace(" >=", ">=")
        query = query.replace(">= ", ">=")
        query = query.replace(" <=", "<=")
        query = query.replace("<= ", "<=")
        query = query.replace(" >", ">")
        query = query.replace("> ", ">")
        query = query.replace(" <", "<")
        query = query.replace("< ", "<")
        query = query.replace("order by", "orderby")

        # splits sql command using space as separator
        query_list = query.split()

        # extract table in query
        if "from" in query_list:
            i = query_list.index("from")
            table = query_list[i + 1]
            commands["from"] = table, i
            # extract join argument
            if "join" in query_list:
                i = query_list.index("join")
                join_table = query_list[i + 1]
                commands["join"] = join_table, i
                if join_table in commands:
                    print("Error : Wrong argument near {}".format(join_table))
                    return False
                if "on" in query_list:
                    i = query_list.index("on")
                    join_column = query_list[i + 1]
                    commands["on"] = join_column, i
                    if join_column in commands:
                        print("Error : Wrong argument near {}".format(join_column))
                        return False
                elif "using" in query_list:
                    i = query_list.index("using")
                    join_column = query_list[i + 1]
                    commands["using"] = join_column, i
                    if join_column in commands:
                        print("Error : Wrong arguments near {}".format(join_column))
                        return False
                else:
                    print("Error : Wrong argument near  {}".format(join_table))
                    return False
        elif "update" in query_list:
            i = query_list.index("update")
            table = query_list[i + 1]
            commands["update"] = table, i
        elif "into" in query_list:
            i = query_list.index("into")
            table = query_list[i + 1]
            commands["into"] = table, i
        else:
            print("Error : Wrong argument near {}".format(table))
            return 0

        # verification if arguments is part of commands
        if table in commands:
            print("Error : wrong argument {}".format(table))
            return 0

        if "select" in query_list:
            i = query_list.index("select")
            columns = query_list[i + 1]
            commands["select"] = columns, i
        elif "update" in query_list:
            i = query_list.index("set")
            set = query_list[i + 1]
            commands["set"] = set, i
        elif "insert" in query_list:
            i = query_list.index("values")
            values = query_list[i + 1]
            commands["values"] = values, i
        elif "delete" in query_list:
            i = query_list.index("from")
            delete = " "
            commands["delete"] = delete, i
        else:
            print("Error : unexpected")
            return False

        # catch where statement
        if "where" in query_list:
            i = query_list.index("where")
            clause = query_list[i + 1]
            commands["where"] = clause, i

        # catch and or statement
        if "and" in query_list:
            i = query_list.index("and")
            clause = query_list[i + 1]
            commands["and"] = clause, i
        elif "or" in query_list:
            i = query_list.index("or")
            clause = query_list[i + 1]
            commands["or"] = clause, i

        # catch order by
        if "orderby" in query_list:
            i = query_list.index("orderby")
            clause = query_list[i + 1]
            commands["order by"] = clause, i

        """ # if(is_query_valid()):
        data = _from()

        if tuple_value(commands["select"]):
            _select(data)
        elif tuple_value(commands["delete"]):
            _delete(data)
        elif tuple_value(commands["into"]):
            _insert(data)
        elif tuple_value(commands["update"]):
            _update(data) """

    except:
        print("Error : Invalid query")
        return False


def importer():
    option = None
    while not (option == "mysql" or option == "csv"):
        print(
            "mysql - Importar do banco MySQL\ncsv   - Importar um arquivo .CSV\n(mysql/csv)")
        option = input(">> ")
    if option == "mysql":
        mysql_imp.importer()
    # elif option == "csv":
        # csv_import.csv_import()
    return


def main():
    # takes query from user
    answer = None
    while not (answer == "i" or answer == "c" or answer == "s"):
        print("i - Importar \nc - Consualtar(query) \ns - sair?\n(i/c/s)")
        answer = input(">> ")
    if answer == "i":
        print("Importar!\n")
        importer()
    elif answer == "c":
        print("Consultar!\n")
        # query()
    elif answer == "s":
        return False

    return True


if __name__ == "__main__":
    while main():
        continue
