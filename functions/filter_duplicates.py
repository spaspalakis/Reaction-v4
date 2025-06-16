import numpy as np

def filter_doubles(overlaps,max_overlaps_idx):

    """
    In this part of code, we try to eliminate the double detection in the same object
    We find unique elements from max_overlaps_idx list , except those with id=0 (newcomers)
    We keep and display the object with the highest confidence 
    eg. In frame 120:
    max_overlaps_idx: [1 3 4 6 9 8 9 0] | len: 8
    So we have double index=9 
    """
    unique_elements, counts = np.unique(max_overlaps_idx, return_counts=True)
    duplicate_elements = unique_elements[counts > 1]
    # print(f'\nduplicate_elements: {duplicate_elements}')
    """
    -elem: the number which is double (eg. number 9 in our example)
    -indices: the index in the max_overlaps_idx list for those two numbers [4,6]
    -highest_overlap_idx: keep the index with the highest confidence
    """
    for elem in duplicate_elements:
        if elem == 0:
            continue
        
        # print(f'elem: {elem}')

        indices = np.where(max_overlaps_idx == elem)[0]
        # print(f'indices: {indices}')

        highest_overlap_idx = indices[np.argmax(overlaps[indices, elem])]
        # print(f'np.argmax(overlaps[indices, elem]: {overlaps[indices, elem]}')
        # print(f"highest_overlap_idx: {highest_overlap_idx} with value: {overlaps[highest_overlap_idx,elem]}")
        #give to the false detection id=-1
        for idx in indices:
            if idx != highest_overlap_idx:
                max_overlaps_idx[idx] = -1


    return max_overlaps_idx