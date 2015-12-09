def is_column_num(column):
    try:
        for v in column:
            float(v)
        return True
    except ValueError:
        return False

def print_dict(dictionary):
    for key, value in dictionary.items():
        print("")
        print(str(key))
        print(str(value))

def cast_list_to_float(l):
    newlist = []
    for el in l:
        newlist.append(float(el)) 
    return newlist

if __name__ == "__main__":
    lst = ["23", "234", "12"]
    print(lst)
    intlist = cast_list_to_int(lst)
    print(intlist)
