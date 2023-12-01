

class Table:
    def __init__(self, table_name: str, column_names: list):

        self.table_name = table_name

        self.total_columns = len(column_names)
        self.column_names = column_names

        self.data = []

    def add_column(self, column_name: str):

        self.total_columns += 1
        self.column_names.append(column_name)

        for row in self.data:
            row[column_name] = None

    def add_row(self, data: dict):
        self.data.append(data)

    def set_column_names(self, column_names: list):
        self.total_columns = len(column_names)
        self.column_names = column_names

    def select(self, selected_fields: list):
        if len(selected_fields) == 0:
            selected_fields = self.column_names
        new_table = Table(self.table_name + "_sel", selected_fields)
        for row in self.data:
            new_row = {}
            for field in selected_fields:
                new_row[field] = row[field]
            new_table.add_row(new_row)
        return new_table
    
    def filter(self, fields: list, operators: list, modifier: str = ''):

        treated_fields = []
        for field in fields:
            if not field in self.column_names:
                if field.split(".")[1] in self.column_names:
                    treated_fields.append(field.split(".")[1])
                else:
                    print(f"ERRO: não há o campo {field} em {self.table_name}")
                    return None
            else:
                treated_fields.append(field)

        new_table = Table(self.table_name + "_fil", self.column_names)
        if modifier == '' or len(treated_fields) == 1:
            for row in self.data:
                if operators[0](row[treated_fields[0]]):
                    new_table.add_row(row)
        elif modifier == 'OR':
            for row in self.data:
                if operators[0](row[treated_fields[0]]) or operators[1](row[treated_fields[1]]):
                    new_table.add_row(row)
        elif modifier == 'AND':
            for row in self.data:
                if operators[0](row[treated_fields[0]]) and operators[1](row[treated_fields[1]]):
                    new_table.add_row(row)
        
        return new_table
    
    def order(self, fields: list, descending: bool):

        treated_fields = []
        for field in fields:
            if not field in self.column_names:
                if field.split(".")[1] in self.column_names:
                    treated_fields.append(field.split(".")[1])
                else:
                    print(f"ERRO: não há o campo {field} em {self.table_name}")
                    return None
            else:
                treated_fields.append(field)

        for field in treated_fields:
            self.data.sort(key = lambda x: x[field], reverse = descending)

    def update(self, fields: list, operators: list, condition_field: str, condition):
        for i in range(len(self.data)):
            if condition(self.data[i][condition_field]):
                for j in range(len(fields)):
                    self.data[i][fields[j]] = operators[j](self.data[i][fields[j]])
    
    def delete(self, condition_field: str, condition):
        self.data = list(filter(lambda x: not condition(x[condition_field]), self.data))





    
            

    

def join_tables(first: Table, second: Table, first_field: str, second_field: str):
    
    if not first_field in first.column_names:
        if first_field.split(".")[1] in first.column_names:
            first_field = first_field.split(".")[1]
        else:
            print(f"ERRO: não há o campo {first_field} em {first.table_name}")
            return None
        
    if not second_field in first.column_names:
        if second_field.split(".")[1] in first.column_names:
            second_field = second_field.split(".")[1]
        else:
            print(f"ERRO: não há o campo {second_field} em {second.table_name}")
            return None
        
    fields = []
    for field in first.column_names:
        if len(field.split(".")) > 1:
            fields.append(field)
        else:
            fields.append(first.table_name + "." + field)
    for field in second.column_names:
        if len(field.split(".")) > 1:
            fields.append(field)
        else:
            fields.append(second.table_name + "." + field)
        

    new_table = Table(first.table_name + "_j_" + second.table_name, fields)

    index = {}
    for row in second.data:
        if row[second_field] in index:
            index[row[second_field]].append(row)
        else:
            index[row[second_field]] = [row]
            
    for row in first.data:
        if  not row[first_field] in index:
            continue
        for adendum in index[row[first_field]]:
            new_row = {}

            for field in first.column_names:
                if len(field.split(".")) > 1:
                    new_row[field] = row[field]
                else:
                    new_row[first.table_name + "." + field] = row[field]
            for field in second.column_names:
                if len(field.split(".")) > 1:
                    new_row[field] = adendum[field]
                else:      
                    new_row[second.table_name + "." + field] = adendum[field]

            new_table.add_row(new_row)

    return new_table

    
