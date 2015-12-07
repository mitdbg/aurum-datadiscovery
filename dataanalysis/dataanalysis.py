from sklearn.neighbors.kde import KernelDensity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm 
from scipy.stats import ks_2samp
import numpy as np
import nltk

def similar_num_columns(columnA, columnB, ncol_dist):
    distA = ncol_dist[columnA]
    distB = ncol_dist[columnB]
    return True

def compare_num_columns_dist_ks(columnA, columnB):
    ''' 
        Kolmogorov-Smirnov test
    '''
    return ks_2samp(columnA, columnB)

def compare_num_columns_dist_odsvm(svm, columnBdata):
    Xnumpy = np.asarray(columnBdata)
    X = Xnumpy.reshape(-1, 1)
    prediction_vector = svm.predict(X)
    return prediction_vector

def get_dist(data_list, method):
    Xnumpy = np.asarray(data_list)
    X = Xnumpy.reshape(-1, 1)
    dist = None
    if method == "kd":
        kde = KernelDensity(kernel='gaussian', bandwidth=0.2).fit(X)
        dist = kde.score_samples(X) 
    elif method == "odsvm":
        svmachine = svm.OneClassSVM(nu=0.1, kernel="rbf", gamma=0.1)
        dist = svmachine.fit(X)
    return dist
    
def get_textual_dist(data_list):
    ''' Get TF-IDF '''
    # merge column into 1 sentence first
    text = ' '.join(data_list)
    tokens = get_tokens(text)
    print('todo')

def get_tokens(text):
    tokens = nltk.word_tokenize(text)
    return tokens
    
def compare_text_columns_dist(docs):
    ''' cosine distance between two vector of hash(words)'''
    vect = TfidfVectorizer(min_df=1)
    tfidf = vect.fit_transform(docs)
    sim = ((tfidf * tfidf.T).A)[0,1]
    #pairwise_similarity = tfidf * tfidf.T
    return sim

def get_sim_vector_numerical(column, ncol_dist):
    value_to_compare = ncol_dist[column]
    vn = dict()
    for key, value in ncol_dist.items():
        test = compare_num_columns_dist_ks(value_to_compare, value) 
        vn[key] = test
    return vn

def get_sim_matrix_numerical(ncol_dist):
    # Pairwise comparison of all numerical column dist. keep them in matrix
    mn = dict() 
    for key, value in ncol_dist.items():
        vn = get_sim_vector_numerical(key, ncol_dist)
        mn[key] = vn
    return mn

def get_sim_vector_text(column, tcol_dist):
    value_to_compare = tcol_dist[column]
    vt = dict()
    for key, value in tcol_dist.items():
        if value_to_compare is not "" and value is not "":
            try:
                sim = compare_text_columns_dist([value_to_compare, value])
            except ValueError:
                sim = "No sim for (" + str(column) + "" + str(key) + ")" 
            vt[key] = sim
    return vt

def get_sim_matrix_text(tcol_dist):
    # Pairwise comparison of all textual column dist. keep them in matrix
    mt = dict() 
    for key, value in tcol_dist.items():
        mt[key] = dict()
        vt = get_sim_vector_text(key, tcol_dist)
        mt[key] = vt
    return mt

def is_column_num(column):
    try:
        for v in column:
            float(v)
        return True
    except ValueError:
        return False

def get_column_signature(column, method):
    dist = None
    if is_column_num(column):
        print('TYPE: num')
        # Get dist only for numerical columns
        dist = get_dist(column, method) 
    else: # only numerical and text columns supported so far
        print('TYPE: text')
        dist = ' '.join(column)
    return dist

def get_columns_signature(columns):
    ncol_dist = dict()
    tcol_dist = dict()
    # Get distribution for numerical columns
    for key, value in columns.items():
        print("")
        print("Kernel for key: " + str(key))
        # the case of non-numerical columns
        if is_column_num(value):
            print('TYPE: num')
            # Get dist only for numerical columns
            dist = get_dist(value, "kd") 
            ncol_dist[key] = dist 
            #print(str(dist))
        else: # only numerical and text columns supported so far
            print('TYPE: text')
            column_repr = ' '.join(value)
            tcol_dist[key] = column_repr
    return (ncol_dist, tcol_dist)

if __name__ == "__main__":
    print("Data analysis")

