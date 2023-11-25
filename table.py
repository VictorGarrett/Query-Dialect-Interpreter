

class Table:
    def __init__(self, column_names: list):

        self.total_columns = len(column_names)
        self.column_names = column_names

        self.data = [{}]

    def add_column(self, column_name: str):

        self.total_columns += 1
        self.column_names.append(column_name)

        for row in self.data:
            row[column_name] = None

    def add_row(self, data: dict):
        self.data.append(data)
    
