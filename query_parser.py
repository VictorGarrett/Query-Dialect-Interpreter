import file_handler
from table import join_tables

def get_operator_lambda(operator: str, value: str):
    try:
        value = int(value)
    except ValueError:
        try:
            value = float(value)
        except ValueError:
            try:
                value = value.strip("\"")
            except ValueError:
                return lambda x: False

    if operator == "=":
        return lambda x: x == value
    if operator == ">":
        return lambda x: x > value
    if operator == "<":
        return lambda x: x < value
    if operator == ">=":
        return lambda x: x >= value
    if operator == "<=":
        return lambda x: x <= value
    if operator == "!=":
        return lambda x: x != value

class Parser:

    def __init__(self, db: str):
        self.database_name = db
        self.keywords = ["SELECIONE", "DE", "UNIAO", "EM", "USANDO", "ONDE", "ORDENAR", "VALORES"]

    def get_parameters_of(self, word: str, word_list: list):
        parameters = []

        if word in word_list:
            word_index = word_list.index(word)
            if word_index+1 >= len(word_list):
                return
            for param in word_list[word_index+1:]:
                #print(f"word {param} in parameters of {word}")
                if param in self.keywords:
                    break
                parameters.append(param.strip(","))

        return parameters

    def set_database_name(self, db: str):
        self.database_name = db

    def handle_select(self, word_list: list):

        parameters_dict = {}

        for keyword in self.keywords:
            parameters_dict[keyword] = self.get_parameters_of(keyword, word_list)

        print(parameters_dict)

        if len(parameters_dict["SELECIONE"]) == 0:
            print("ERRO: é preciso selecionar alguma coluna")
            return
        for column in parameters_dict["SELECIONE"]:
            print(column)
            print(column.split("."))
            if len(column.split(".")) != 2:
                print("ERRO: é preciso selecionar as colunas com a forma: <nome_tabela>.<nome_coluna>")  
                return  
        if len(parameters_dict["DE"]) == 0:
            print("ERRO: é preciso selecionar de alguma tabela")
            return
        if len(parameters_dict["DE"]) > 1:
            print("ERRO: não é possivel selecionar de mais de uma tabela, utilize UNIAO")
            return

        first_table = file_handler.load_table_from_database(parameters_dict["DE"][0], self.database_name)

        join_done = False
        if len(parameters_dict["UNIAO"]) > 0:
            join_done = True
            if len(parameters_dict["EM"]) == 3:
                second_table = file_handler.load_table_from_database(parameters_dict["UNIAO"][0], self.database_name)
                first_table = join_tables(first_table, second_table, parameters_dict["EM"][0], parameters_dict["EM"][2])
            elif len(parameters_dict["USANDO"]) == 1:
                second_table = file_handler.load_table_from_database(parameters_dict["UNIAO"][0], self.database_name)
                first_table = join_tables(first_table, second_table, parameters_dict["USANDO"][0], parameters_dict["USANDO"][0])
            else:
                print("ERRO: uso incorreto de UNIAO, as sintaxes possíveis são:\n <a> UNIAO <b> EM <col_a> = <col_b>\n <a> UNIAO <b> USANDO <col_ab>")
                return
        
        field_names = []
        if join_done:
            field_names = parameters_dict["SELECIONE"]
        else:
            for field in parameters_dict["SELECIONE"]:
                field_names.append(field.split(".")[1])

        print("field names")
        print(field_names)
        first_table = first_table.select(field_names)


        if len(parameters_dict["ONDE"]) != 0:
            if len(parameters_dict["ONDE"]) == 7:
                operators = [get_operator_lambda(parameters_dict["ONDE"][1], parameters_dict["ONDE"][2]), get_operator_lambda(parameters_dict["ONDE"][5], parameters_dict["ONDE"][6])]
                if parameters_dict["ONDE"][3] == 'e':
                    connector = "AND"
                elif parameters_dict["ONDE"][3] == 'ou':
                    connector = "OR"
                else:
                    print("ERRO: conector pode apenas ter os valores \"e\" ou \"ou\"")
                    return
                
                first_table = first_table.filter([parameters_dict["ONDE"][0], parameters_dict["ONDE"][4]], operators, connector)
            elif len(parameters_dict["ONDE"]) == 3:
                operators = [get_operator_lambda(parameters_dict["ONDE"][1], parameters_dict["ONDE"][2])]
                first_table = first_table.filter([parameters_dict["ONDE"][0]], operators, "")
            else:
                print("ERRO: as sintaxes possiveis sao:\nONDE <campo1> <operador1> <valor1>\nONDE <campo1> <operador1> <valor1> <conector> <campo2> <operador2> <valor2>")
                return
        
        if len(parameters_dict["ORDENAR"]) != 0:
            if len(parameters_dict["ORDENAR"]) == 2:
                if parameters_dict["ORDENAR"][1] == "DECRES":
                    dec = True
                elif parameters_dict["ORDENAR"][1] == "CRES":
                    dec = False
                else:
                     print("ERRO: as opções de ordenação são CRES e DECRES")
                first_table.order([parameters_dict["ORDENAR"][0]], dec)
            elif len(parameters_dict["ORDENAR"]) == 3:
                if parameters_dict["ORDENAR"][2] == "DECRES":
                    dec = True
                elif parameters_dict["ORDENAR"][2] == "CRES":
                    dec = False
                else:
                     print("ERRO: as opções de ordenação são CRES e DECRES")
                first_table.order([parameters_dict["ORDENAR"][0], parameters_dict["ORDENAR"][1]], dec)
            else:
                print("ERRO: as sintaxes possiveis sao:\nORDENAR <campo1> <CRES/DECRES>\nORDENAR <campo1> <campo2> <CRES/DECRES>")
        print(first_table.data)
        file_handler.output_table(first_table)

    def handle_insert(self, word_list):
        values = self.get_parameters_of("VALORES", word_list)
        table_name = self.get_parameters_of("EM", word_list)

        table = file_handler.load_table_from_database(table_name, self.database_name)
        print(values)
        print(table_name)
        clean_values = []
        for value in values:
            clean_values.append(value.strip("()"))
        
        if len(clean_values) % table.total_columns != 0:
            print("numero incorreto de valores nas tuplas")
            return
        
        column_values = {}
        for column_name in table.column_names:
            column_values[column_name] = []
        for i in range(len(values)):
            column_values[table.column_names[i%table.total_columns]].append(values[i])
            



    def parse_query(self, query_string: str):
        query_words = query_string.split(' ')
        
        if query_words == 0:
            print("consulta não reconhecida!")
            return
        
        if query_words[0] == "SELECIONE":
            self.handle_select(query_words)
        elif query_words[0] == "INSIRA":
            self.handle_insert(query_words)
        elif query_words[0] == "ATUALIZE":
            self.handle_update(query_words)
        elif query_words[0] == "DELETE":
            self.handle_delete(query_words)
        else:
            print("consulta não reconhecida!")
        return