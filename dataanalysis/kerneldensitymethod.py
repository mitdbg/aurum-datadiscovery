from sklearn.cluster import KMeans

from dataanalysis import jenks as J

# PROTOTYPE_ yet to test
def kerndens(columnA, columnB):
    k_d = None
    if U.is_column_num(columnA):
        k_d = extract_kernels_for_num_col(columnA, columnB)
    else:
        k_d = extract_kernels_for_text_col(columnA, columnB)
    return k_d

def extract_kernel_for_num_col(columnA, columnB):
    # Create a map from kernel to set
    k_d = None
    kernel_instances = dict()
    # extract kernels of columnA and keep instances of B
    # stored in the dict
    # For Jenks thresholds, data must be ordered
    cA, cB = (list(t) for t in zip(*sorted(zip(columnA, columnB))))
    kernels = J.jenks(cA, C.kernel_density["kernels"])
    start_idx = 0
    for k in kernels:
        end_idx = cA.index(k)
        b_samples = cB[start_idx : end_idx]
        kernel_instances[k] = b_samples
        start_idx = end_idx + 1
    k_d = get_group_signatures(kernel_instances)
    return k_d
 
def extract_kernel_for_text_col(columnA, columnB):
    print("TODO")

def get_group_signatures(kernel_instances):
    k_d = dict()
    for k, group in kernel_instances.items():
        sig = DA.get_column_signature(
                group, 
                C.kernel_density["density_method"]
        )
        k_d[k] = sig
    return k_d


if __name__ == "__main__":
    print("testing kernel density method")
