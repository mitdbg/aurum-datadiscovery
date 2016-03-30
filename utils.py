def is_column_num(column):
    value_error_count = 0
    type_error_count = 0
    threshold = 0.3
    for v in column:
        try:
            float(v)
        except ValueError:
            ++value_error_count
        except TypeError:
            ++type_error_count
    return (value_error_count+type_error_count)< threshold*len(column)

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

if __name__ == "__main__":
    lst = ["23", "234", "12"]
    print(lst)
    intlist = cast_list_to_float(lst)
    print(intlist)

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
  except TypeError:
      return False