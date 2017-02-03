import pickle


def serialize_object(obj, path):
    f = open(path, 'wb')
    pickle.dump(obj, f)
    f.close()


def deserialize_object(path):
    f = open(path, 'rb')
    obj = pickle.load(f)
    return obj

if __name__ == "__main__":
    print("Input output")