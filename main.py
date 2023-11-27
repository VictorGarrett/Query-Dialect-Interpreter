import csv_importer as importer
from table import join_tables

print("importing")
tab = importer.import_table("./cities2.csv", "taba", [])
tabb = importer.import_table("./cities2.csv", "tabb", [])

importer.export_table("./cities3.csv", tab)

print(tab.data)

tab2 = tab.select(["City","State"])

print(tab2.data)

tab_fil = tab.filter(["LatD", "LatM"], [lambda x: x > 40, lambda x: x > 20], "AND")
importer.export_table("./cities_fil.csv", tab_fil)

tab_fil.order(["LatM", "LatD"], False)
importer.export_table("./cities_fil_ord.csv", tab_fil)


tab3 = join_tables(tab, tab2, "City", "City")

print("\n\n JOINED \n\n")
print(tab3.data)
importer.export_table("./citiesjoin.csv", tab3)