import csv

def load_locations():
    with open('csv/locations.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            print(row)  