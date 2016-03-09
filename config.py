# General properties of the prototype

# List of valid signature and similarity methods 
# (i.e. with an implementation)

valid_signature_methods = ["raw", "kd", "odsvm"]
valid_similarity_methods = ["ks"]

preferred_num_method = "raw"
preferred_text_method = "vector"

# Configuration specific to each signature method

kd =    {
        "kernel":"gaussian",
        "bandwidth":0.2
        }

odsvm = {
        "nu":0.1,
        "kernel":"rbf",
        "gamma":0.1
        }

# Configuration specific to each similarity method

ks =    {
        "dvalue":0.5,
        "pvalue":0.001
        }

cosine= {
        "threshold":0.2
        }

# Configs related to joint signatures
all_jsignatures = ["pearson", "kernel-density"]

# List those methods that require the same sample size to work
jsignature_samplesize = ["pearson"]

pearson_sim = {
        "threshold":0.05
    }

kernel_density = {
    "kernels" : 4,
    "density_method": "raw"
    }

# Simrank parameters
sr_maxiter = 1000
sr_eps = 1e-4
sr_c = 0.8
simrank_sim_threshold = 0.2

# Overlap parameters
join_overlap_th = 0.4

# Schema similarity parameters
max_distance_schema_similarity = 10

# Serde parameters
serdepath = "./data"
signcollectionfile = "sigcolfile.pickle"
graphfile = "graphfile.pickle"
graphcachedfile = "graphcachedfile.pickle"
datasetcolsfile = "datasetcols.pickle"
simrankfile = "simrankfile.pickle"
jgraphfile = "jgraphfile.pickle"

# DB connection
db_location = "mongodb://localhost:27017"

################
# Cluster configs
################
max_future_list_size = 10
parallel_index_batch_size = 20


