def print_dict(dictionary):
    for key, value in dictionary.items():
        print("")
        print(str(key))
        print(str(value))

def cast_list_to_int(list):
    newlist = []
    for el in list:
        newlist.append(int(el)) 
    return newlist

if __name__ == "__main__":
    lst = ["23", "234", "12"]
    print(lst)
    intlist = cast_list_to_int(lst)
    print(intlist)
