import csv_importer as importer

print("importing")
tab = importer.import_table("./cities.csv", [])

print(tab.data)