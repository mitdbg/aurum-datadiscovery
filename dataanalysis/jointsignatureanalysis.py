from scipy.stats.stats import pearsonr

import config as C
import utils as U

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
        # this method requires both columns to be of same type
        if same_type_num(columnA, columnB):
            return pearsonr(columnA, columnB)
        return False

def same_type_num(a, b):
    if U.is_column_num(a) and U.is_column_num(b):
        return True
    return False

'''
 API based on joint signatures
'''

def get_similar_pairs(jsignature, columns, method):
    '''
        Obtain all pairs of columns with similar signature
        to the one provided
    '''
    banned = [] # FIXME: keep set with custom eq
    sim_pairs = [] # list of tuples
    for columnA in columns:
        for columnB in columns:
            colA_data = columns[columnA]
            colB_data = columns[columnB]
            jsig = get_jsignature(colA_data, colB_data, method)    
            if jsig is not False:
                sim = compare_jsignatures(jsig, jsignature, method)
                if sim:
                    newpair = (columnA, columnB)
                    if not repeated_pair(newpair, banned):
                        sim_pairs.append(newpair)
                    banned.append(newpair)
    return sim_pairs

def repeated_pair(pair, banned):
    (ca, cb) = pair
    for (_ca, _cb) in banned:
        if (ca is _ca and cb is _cb) or \
        (ca is _cb and cb is _ca) or \
        ca is cb:
            return True 
    return False

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
        print(str(dif) + " < " + str(C.pearson_sim["threshold"]))
        if dif < C.pearson_sim["threshold"]:
            return True
    return False

def get_columns_similar_to_jsignature(
            columnA, 
            jsignature, 
            columns, 
            method):
    '''
        Given columnA as reference and its jsignature, computed
        with respect to other columns, obtain any columns that
        return a similar jsignature
    '''
    sim_columns = [] # list of tuples
    for key, columnB in columns.items():
        jsig = get_jsignature(columnA, columnB, method)  
        if jsig is not False:
            sim = compare_jsignatures(jsig, jsignature, method)
            if sim:
                sim_columns.append(key)
    return sim_columns


if __name__ == "__main__":
    print("Joint signature analysis")
