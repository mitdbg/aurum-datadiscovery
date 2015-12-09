from scipy.stats.stats import pearsonr

import config as C

'''
Functions to obtain joint signatures
'''

def get_jsignature(columnA, columnB, method):
    '''
        Obtain a join signature of two columns
    '''
    if method not in C.all_jsignatures:
        print("ERROR: Unrecognized method " + str(method))
        return False
    if method in C.jsignature_samplesize:
        if len(columnA) is not len(columnB):
            print("ERROR: " + " requires same sample size")
            return False
    # Obtain signature given the method
    if method is "pearson":
        correlation = pearsonr(columnA, columnB)    
        return correlation

'''
 API based on joint signatures
'''

def get_similar_pairs(jsignature, columns, method):
    '''
        Obtain all pairs of columns with similar signature
        to the one provided
    '''
    sim_pairs = [] # list of tuples
    for columnA in columns:
        for columnB in columns:
            jsig = get_jsignature(columnA, columnB, method)    
            if jsig is not False:
                sim = compare_jsignatures(jsig, jsignature, method)
                if sim:
                    sim_pairs.append((columnA, columnB))
    return sim_pairs

def compare_jsignatures(jsignatureA, jsignatureB, method):
    '''
        Compares and returns similarity value between both
        joint signatures according to the provided method
    '''
    if method is "pearson":
        # FIXME: probably not best way of comparing pearson co
        (pearsonA, pA) = jsignatureA
        (pearsonB, pB) = jsignatureB
        dif = abs(pearsonA - pearsonB)
        if dif < C.pearson_sim["threshold"]:
            return True
    return False

def get_columns_similar_to_jsignature(columnA, jsignature, columns):
    '''
        Given columnA as reference and its jsignature, computed
        with respect to other columns, obtain any columns that
        return a similar jsignature
    '''
    sim_columns = [] # list of tuples
    for columnB in columns:
        jsig = get_jsignature(columnA, columnB, method)  
        sim = compare_jsignatures(jsig, jsignature, method)
        if sim:
            sim_columns.append(columnB)
    return sim_columns


if __name__ == "__main__":
    print("Joint signature analysis")
