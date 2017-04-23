## Generate some simple test data for sanity check

import numpy as np
from sklearn.utils.extmath import cartesian
import string, argparse

def make_tensor_1block(tensor_size, block_size, N, low_dens=1, high_dens=100):
    """
    Make a N-way tensor containing 1 dense block.
    """
    ## dense block
    dim_values = [[string.ascii_letters[n] + i for i in map(str, range(block_size))] 
                    for n in range(N)]
    block = np.array(cartesian(dim_values + [[high_dens]]))
    ## remaining tensors
    # other_dim_values = [[string.ascii_letters[n] + i for i in map(str, range(block_size, tensor_size))] 
    #             for n in range(N)]
    # other = np.array(cartesian(other_dim_values + [[low_dens]]))
    other = [[]*(N+1) for row in range(tensor_size-block_size)]
    for row in range(tensor_size-block_size):
        other[row] = [string.ascii_letters[n] + str(block_size+row) for n in range(N)] + [low_dens]

    return np.concatenate((block, other), axis=0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-tensor", "--tensor", type=int, default=20)
    parser.add_argument("-block", "--block", type=int, default=5)
    parser.add_argument("-N", "--N", type=int, default=3)
    parser.add_argument("-out", "--out_file", type=str, default="tests/test_data.csv")
    args = parser.parse_args()

    tensor = make_tensor_1block(tensor_size=args.tensor, block_size=args.block, N=args.N)
    np.savetxt(args.out_file, tensor, fmt='%s', delimiter=',')
    

    
