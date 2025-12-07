from problem2 import construct_hashmap_problem2

# Problem 3 implementation hashmap: This implementation is directly join the line query from left to right using hashmap.
def construct_hashmap_problem3(arr, index):
    """
    Docstring for construct_hashmap_problem3
    
    :param arr: a 2D array
    :param index: the index to be used as key.
    :return: a hashmap where the keys are the elements at the given index, and values are lists of remaining elements in the tuple.
    """
    h = {}
    for i in range(len(arr)):
        cur_element = arr[i]
        key = cur_element[index]
        val = cur_element[:index] + cur_element[index+1:]
        if key not in h:
            h[key] = [val]
        else:
            h[key].append(val)
    return h

def inner_join_left_to_right_efficient(db):
    cur_result = db[0] # Initiate the current result as the first table
    for i in range(1, len(db)):
        temp_result = []
        cur_db = db[i] # Current database
        # Skip if current result is empty
        if len(cur_result) == 0:
            continue
        
        # Construct hashmap for current result based on the last attribute (joining attribute)
        cur_joining_attribute_val = construct_hashmap_problem3(cur_result, len(cur_result[0]) - 1)
        
        # Construct hashmap for current db based on the first attribute (joining attribute)
        h = construct_hashmap_problem2(cur_db, 0)
        
        # Perform the join using the two hashmaps
        for j in cur_joining_attribute_val:
            if j in h:
                for val in cur_joining_attribute_val[j]:
                    for tup in h[j]:
                        temp_result.append(val + [j, tup])
                        
        # Set current result to temp result
        cur_result = temp_result
    return cur_result