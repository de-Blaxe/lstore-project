tail_set = [[1,2,3],  # tail row 0
            [1,2,3],  # tail row 1
            [1,2,3]]  # tail row 2

call extend tail set
    tail_set.append([1,2,3]) # tail row 3

candidates = tail_set
merge_queue = candidates[0]

