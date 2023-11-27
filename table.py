

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
        new_table = Table(self.table_name + "_sel", selected_fields)
        for row in self.data:
            new_row = {}
            for field in selected_fields:
                new_row[field] = row[field]
            new_table.add_row(new_row)
        return new_table
    
    def filter(self, fields: list, operators: list, modifier: str = ''):

        new_table = Table(self.table_name + "_fil", self.column_names)
        if modifier == '' or len(fields) == 1:
            for row in self.data:
                if operators[0](row[fields[0]]):
                    new_table.add_row(row)
        elif modifier == 'OR':
            for row in self.data:
                if operators[0](row[fields[0]]) or operators[1](row[fields[1]]):
                    new_table.add_row(row)
        elif modifier == 'AND':
            for row in self.data:
                if operators[0](row[fields[0]]) and operators[1](row[fields[1]]):
                    new_table.add_row(row)
        
        return new_table
    
    def order(self, fields: list, descending: bool):
        for field in fields:
            self.data.sort(key = lambda x: x[field], reverse = descending)


    
            

    

def join_tables(first: Table, second: Table, first_field: str, second_field: str):
    
    fields = []

    for field in first.column_names:
        fields.append(first.table_name + "." + field)
    for field in second.column_names:
        fields.append(second.table_name + "." + field)

    new_table = Table(first.table_name + "_j_" + second.table_name, fields)

    index = {}
    for row in second.data:
        if row[second_field] in index:
            index[row[second_field]].append(row)
        else:
            index[row[second_field]] = [row]
            
    for row in first.data:
        for adendum in index[row[first_field]]:
            new_row = {}

            for field in first.column_names:
                new_row[first.table_name + "." + field] = row[field]
            for field in second.column_names:      
                new_row[second.table_name + "." + field] = adendum[field]

            new_table.add_row(new_row)

    return new_table

    
