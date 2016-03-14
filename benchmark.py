import time
from dataanalysis import dataanalysis as da

def test_num_sim(col1, col2, it):
    start = time.time()
    for i in range(it):
        sim = da.compare_pair_num_columns(col1, col2)
    end = time.time()
    total = end - start
    print("Num sim for iterations: "+str(it)+" took: "+str(total))

def test_text_sim(col1, col2, it):
    start = time.time()
    for i in range(it):
        #sim = da.compare_pair_text_columns(col1, col2)
        sim = da._compare_text_columns_dist(col1, col2)
    end = time.time()
    total = end - start
    print("Text sim for iterations: "+str(it)+" took: "+str(total))

def returnTFIDF(docs):
    return da.get_tfidf_docs(docs)

if __name__ == "__main__":
    col = [x for x in range(50)]
    col1 = col[:10]
    col2 = col[10:20]
    col3 = col[20:30]
    col4 = col[30:40]
    col5 = col[40:]
    c1 = [str(x) for x in col1]
    c2 = [str(x) for x in col2]
    c3 = [str(x) for x in col3]
    c4 = [str(x) for x in col4]
    c5 = [str(x) for x in col5]
    it = 10
    #test_num_sim(col1, col2, it)
    #test_text_sim(col1, col2, it)
    allcols = [c1,c2,c3,c4,c5]
    docs = [' '.join(c) for c in allcols]
    tfidf = returnTFIDF(docs)
    print(str(tfidf))
