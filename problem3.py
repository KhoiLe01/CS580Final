from Problem1 import hash_join

def problem3_algo(db):
    """
    Docstring for problem3_algo
    
    :param db: database as 3D array. Each element is a 2D array representing a relation. Assume that there are at least 2 relations.
    """
    
    # Join R1 with R2
    current_result = hash_join(db[0], db[1])
    current_result = [list(i) for i in current_result] # convert tuple to list for consistency
    
    # For each i from 3 to k
    for i in range(2, len(db)):
        temp_result = []
        
        # Construct hashmap for R_i
        h = {}
        for (b, c) in db[i]:
            if b not in h:
                h[b] = []
            h[b].append((b, c))
        
        # Join the current result with R_i using the hashmap above
        for data in current_result:
            joining_attribute = data[-1]
            if joining_attribute in h:
                for (_, c) in h[joining_attribute]:
                    temp_result.append(data + [c])
        
        # Update the current result
        current_result = temp_result
    
    return current_result