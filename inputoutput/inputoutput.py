import sys
import os
from os import listdir, path
import csv
import _csv
import dataset
import config as C

from collections import defaultdict


def get_files_in_dir(path):
    '''
    Return all non-hidden files in a directory
    '''
    onlynonhiddenfiles = []
    for f in listdir(path):
        if not f.startswith('.'):
            path_to_file = path + "/" + f
            onlynonhiddenfiles.append(path_to_file)
    return onlynonhiddenfiles


def get_columns_from_csv_file(filename):
    '''
    Given a CSV file returns a dictionary where
    key -> (filename, columna)
    value -> [values]
    '''
    #max_values = C.max_values_per_column
    #current_values = 0
    columns = defaultdict(list)
    with open(filename, encoding="latin-1") as f:
        reader = csv.DictReader(f)
        try:
            for row in reader:
                #current_values = current_values + 1
                for (k, v) in row.items():
                    # TODO: ugly the need to call this each time
                    f_name = os.path.basename(f.name)
                    column_name = (f_name, k)
                    columns[column_name].append(v)
                #if max_values != -1:
                #    if current_values > max_values:
                    # early-termination for reaching max values
                #        return columns
        except _csv.Error:
            print(
                "skipping file, as critical problems found while reading file: " + str(filename))
    return columns


def get_column_iterator_csv(files):
    '''
    Given a collection of files returns a dictionary
    with all the columns and values in the dataset
    '''
    columns = defaultdict(list)
    for filename in files:
        print("Processing ... " + str(filename))
        file_columns = get_columns_from_csv_file(filename)
        columns.update(file_columns)
    return columns


def get_header_csv(filename):
    '''
    Given a CSV file, it returns the first row,
    assuming that is the header
    '''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            return row

'''
TODO: is this used?
'''


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


def get_tables_from_db(dbpath):
    con = dataset.connect(dbpath)
    tables = []
    table = con.load_table('all_tables')

    rows = table.all()
    for row in rows:
        tables.append(row['table_name'])

    con.close()
    return tables


def get_columns_from_db(table, dbpath):
    con = dataset.connect(dbpath)
    maintable = con.load_table('all_tables')
    results = maintable.find(table_name=table)
    header = []
    for result in results:
        header.append(results['column_name'])
    columns = defaultdict(list)

    datatable = con.load_table(table)
    rows = datatable.find(_limit=None)
    for row in rows:
        for k in header:
            columns[k].append(row[k])
    return columns
