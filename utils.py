def is_column_num(column):
    print(str(column))
    value_error_count = 0
    type_error_count = 0
    threshold = 0.3
    for v in column:
        try:
            float(v)
        except ValueError:
            value_error_count = value_error_count + 1
        except TypeError:
            type_error_count = type_error_count + 1
    total_errors = (value_error_count + type_error_count)
    th_value = threshold * len(column)
    pass_threshold = total_errors < th_value
    print(str("T_ERRRORS: " + str(total_errors)))
    print(str("th_value: " + str(th_value)))
    return pass_threshold

def print_dict(dictionary):
    for key, value in dictionary.items():
        print("")
        print(str(key))
        print(str(value))

def cast_list_to_float(l):
    newlist = []
    for el in l:
        if isfloat(el):
            newel = float(el)
            newlist.append(newel)
    return newlist

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
    except TypeError:
        return False

if __name__ == "__main__":
    lst = ["23", "234", "12"]
    print(lst)
    intlist = cast_list_to_float(lst)
    print(intlist)

