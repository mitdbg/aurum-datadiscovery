import sys
import os
from os import listdir, path
import csv
from collections import defaultdict

def get_files_in_dir(path):
    onlynonhiddenfiles = []
    for f in listdir(path):
        if not f.startswith('.'):
            path_to_file = path + "/" + f
            onlynonhiddenfiles.append(path_to_file)
    return onlynonhiddenfiles 

def get_column_iterator_csv(files):
    columns = defaultdict(list)
    for filename in files:
        #print("Processing ... " + str(filename))
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                for (k,v) in row.items():
                    f_name = os.path.basename(f.name)
                    column_name = (f_name, k)
                    columns[column_name].append(v)
    return columns

def get_header_csv(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            return row

def get_row_iterator_csv(filename):
    rows = []
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
    return rows 

if __name__ == "__main__":
    filename = sys.argv[1]
    print("File: " + str(filename))
    header = get_header_csv(filename)
    print("HEADER: " + str(header))
    rows = get_row_iterator_csv(filename)
    # print(str(len(rows)) + " rows in the file")
    for row in rows:
        print(str(row))
    print("Columns in the file")
    columns = get_column_iterator_csv(filename)
    print(str(columns))

