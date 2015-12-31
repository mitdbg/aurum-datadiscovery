from sklearn.neighbors.kde import KernelDensity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm 
from scipy.stats import ks_2samp
import numpy as np
import nltk

import config as C
import utils as U

def compare_num_columns_dist(columnA, columnB, method):
    if method is "ks":
        return compare_num_columns_dist_ks(columnA, columnB)
    if method is "odsvm":
        return compare_num_columns_dist_odsvm(columnA, columnB)

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

def get_sim_items_ks(key, ncol_dist):
    sim_columns = []
    sim_vector = get_sim_vector_numerical(
            key, 
            ncol_dist,
            "ks")
    for filekey, sim  in sim_vector.items():
        dvalue = sim[0]
        pvalue = sim[1]
        if dvalue < C.ks["dvalue"] \
            and \
        pvalue > C.ks["pvalue"]:
            sim_columns.append(filekey)
    return sim_columns

def get_dist(data_list, method):
    Xnumpy = np.asarray(data_list)
    X = Xnumpy.reshape(-1, 1)
    dist = None
    if method == "raw":
        dist = data_list # raw column data
    if method == "kd":
        kde = KernelDensity(
            kernel = C.kd["kernel"], 
            bandwidth = C.kd["bandwidth"]
        ).fit(X)
        dist = kde.score_samples(X) 
    elif method == "odsvm":
        svmachine = svm.OneClassSVM(
            nu = C.odsvm["nu"], 
            kernel = C.odsvm["kernel"], 
            gamma = C.odsvm["gamma"]
        )
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
    #print(str(tfidf * tfidf.T))
    #pairwise_similarity = tfidf * tfidf.T
    return sim

def get_sim_vector_numerical(column, ncol_dist, method):
    value_to_compare = ncol_dist[column]
    vn = dict()
    for key, value in ncol_dist.items():
        test = compare_num_columns_dist(
                value_to_compare, 
                value, 
                method) 
        vn[key] = test
    return vn

def get_sim_matrix_numerical(ncol_dist, method):
    '''
         Pairwise comparison of all numerical column dist. 
         keep them in matrix
    '''
    mn = dict() 
    for key, value in ncol_dist.items():
        vn = get_sim_vector_numerical(key, ncol_dist, method)
        mn[key] = vn
    return mn

def get_sim_vector_text(column, tcol_dist):
    value_to_compare = tcol_dist[column]
    vt = dict()
    for key, value in tcol_dist.items():
        if value_to_compare is not "" and value is not "":
            try:
                sim = compare_text_columns_dist(
                    [value_to_compare, value]
                )
                vt[key] = sim
            except ValueError:
                print("No sim for (" + str(column) + \
                        "" + str(key) + ")")
                vt[key] = -1
    return vt

def get_sim_matrix_text(tcol_dist):
    # Pairwise comparison of all textual column dist. keep them in matrix
    mt = dict() 
    for key, value in tcol_dist.items():
        mt[key] = dict()
        vt = get_sim_vector_text(key, tcol_dist)
        mt[key] = vt
    return mt

def get_column_signature(column, method):
    dist = None
    tcols = 0
    ncols = 0
    if U.is_column_num(column):
        #print('TYPE: num')
        # Get dist only for numerical columns
        dist = get_dist(column, method) 
        ncols = ncols + 1
    else: # only numerical and text columns supported so far
        #print('TYPE: text')
        dist = ' '.join(column)
        tcols = tcols + 1
    print("Num. columns: " + str(ncols))
    print("Text columns: " + str(tcols))
    return dist

def get_columns_signature(columns, method):
    ncol_dist = dict()
    tcol_dist = dict()
    ncols = 0
    tcols = 0
    # Get distribution for numerical columns
    for key, value in columns.items():
        # the case of non-numerical columns
        if U.is_column_num(value):
            # Get dist only for numerical columns
            dist = get_dist(value, method) 
            ncol_dist[key] = dist 
            ncols = ncols + 1
        else: # only numerical and text columns supported so far
            column_repr = ' '.join(value)
            tcol_dist[key] = column_repr
            tcols = tcols + 1
    print("Num. columns: " + str(ncols))
    print("Text columns: " + str(tcols))
    return (ncol_dist, tcol_dist)

if __name__ == "__main__":
    print("Data analysis")

