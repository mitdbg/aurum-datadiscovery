# General properties of the prototype

# List of valid signature and similarity methods (i.e. with an implementation)
valid_signature_methods = ["raw", "kd", "odsvm"]
valid_similarity_methods = ["ks"]

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

jsignature_samplesize = ["pearson"]

pearson_sim = {
        "threshold":0.05
    }

kernel_density = {
    "kernels" : 4
    }
