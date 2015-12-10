import json
from pprint import pprint as pp

def jenks_matrices_init(data, n_classes):
    #fill the matrices with data+1 arrays of n_classes 0s
    lower_class_limits = []
    variance_combinations = []
    for i in range(0, len(data)+1):
        temp1 = []
        temp2 = []
        for j in range(0, n_classes+1):
            temp1.append(0.)
            temp2.append(0.)
        lower_class_limits.append(temp1)
        variance_combinations.append(temp2)

    inf = float('inf')
    for i in range(1, n_classes+1):
        lower_class_limits[1][i] = 1.
        variance_combinations[1][i] = 0.
        for j in range(2, len(data)+1):
            variance_combinations[j][i] = inf

    return lower_class_limits, variance_combinations

def jenks_matrices(data, n_classes):
    lower_class_limits, variance_combinations = jenks_matrices_init(
            data,
            n_classes)

    variance = 0.0
    for l in range(2, len(data)+1):
        sum = 0.0
        sum_squares = 0.0
        w = 0.0
        for m in range(1, l+1):
            # `III` originally
            lower_class_limit = l - m + 1
            val = data[lower_class_limit-1]

            # here we're estimating variance for each potential classing
            # of the data, for each potential number of classes. `w`
            # is the number of data points considered so far.
            w += 1

            # increase the current sum and sum-of-squares
            sum += val
            sum_squares += val * val

            # the variance at this point in the sequence is the difference
            # between the sum of squares and the total x 2, over the number
            # of samples.
            variance = sum_squares - (sum * sum) / w

            i4 = lower_class_limit - 1

            if i4 != 0:
                for j in range(2, n_classes+1):
                    if variance_combinations[l][j] >= (variance + variance_combinations[i4][j - 1]):
                        lower_class_limits[l][j] = lower_class_limit
                        variance_combinations[l][j] = variance + variance_combinations[i4][j - 1]

        lower_class_limits[l][1] = 1.
        variance_combinations[l][1] = variance

    return lower_class_limits, variance_combinations

def get_jenks_breaks(data, lower_class_limits, n_classes):
    k = len(data) - 1
    kclass = [0.] * (n_classes+1)
    countNum = n_classes

    kclass[n_classes] = data[len(data) - 1]
    kclass[0] = data[0]

    while countNum > 1:
        elt = int(lower_class_limits[k][countNum] - 2)
        kclass[countNum - 1] = data[elt]
        k = int(lower_class_limits[k][countNum] - 1)
        countNum -= 1

    return kclass

def jenks(data, n_classes):
    if n_classes > len(data): return

    data.sort()

    lower_class_limits, _ = jenks_matrices(data, n_classes)

    return get_jenks_breaks(data, lower_class_limits, n_classes)

def main():
    pp(jenks([1,2,2,3,12,2,2,1,2,11,1,11,1,1,1,1,1,1,1,1,2,1,9,8,9,8,9,8,9,8,9,8,9,8,9,8,1,2,1,2,1,2,1,2],4))


if __name__ == "__main__":
    main()
