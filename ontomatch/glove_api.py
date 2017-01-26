import numpy as np

w = None
vocab = None
ivocab = None


def get_embedding_for_word(word):
    if word in vocab:
        return w[vocab[word], :]
    else:
        return None


def semantic_distance(v1, v2):
    sim = np.dot(v1, v2.T)
    return sim


def load_vocab(vocabfile):
    with open(vocabfile, 'r') as f:
        words = [x.rstrip().split(' ')[0] for x in f.readlines()]
    with open(vocabfile, 'r') as f:
        vectors = {}
        for line in f:
            vals = line.rstrip().split(' ')
            vectors[vals[0]] = [float(x) for x in vals[1:]]

    vocab_size = len(words)
    vocab = {w: idx for idx, w in enumerate(words)}
    ivocab = {idx: w for idx, w in enumerate(words)}

    vector_dim = len(vectors[ivocab[0]])
    W = np.zeros((vocab_size, vector_dim))
    for word, v in vectors.items():
        if word == '<unk>':
            continue
        W[vocab[word], :] = v

    # normalize each word vector to unit variance
    W_norm = np.zeros(W.shape)
    d = (np.sum(W ** 2, 1) ** (0.5))
    W_norm = (W.T / d).T
    return (W_norm, vocab, ivocab)


def load_model(path_to_vocab):
    global w, vocab, ivocab
    w, vocab, ivocab = load_vocab(path_to_vocab)

    print(str(len(w)))
    print(str(len(vocab)))
    print(str(len(ivocab)))

if __name__ == "__main__":
    print("glove model")
    load_model()

    print(str(semantic_distance("dog", "animal")))
    print(str(semantic_distance("cat", "animal")))
    print(str(semantic_distance("warehouse", "address")))
    print(str(semantic_distance("building", "name")))

