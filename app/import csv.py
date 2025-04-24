import csv

with open("sinopses_jogos_igdb.csv", encoding="utf-8") as f:
    reader = csv.reader(f)
    for i, row in enumerate(reader, start=1):
        if len(row) != 2:
            print(f"⚠️ Problema na linha {i}: {row}")