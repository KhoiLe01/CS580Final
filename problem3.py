from Problem1 import hash_join

def problem3_algo(db):
    """
    Docstring for problem3_algo
    
    :param db: database as 3D array. Each element is a 2D array representing a relation. Assume that there are at least 2 relations.
    """
    current_result = hash_join(db[0], db[1])
    current_result = [list(i) for i in current_result] # convert tuple to list for consistency
    for i in range(2, len(db)):
        temp_result = []
        h = {}
        for (b, c) in db[i]:
            if b not in h:
                h[b] = []
            h[b].append((b, c))
        
        for data in current_result:
            joining_attribute = data[-1]
            if joining_attribute in h:
                for (_, c) in h[joining_attribute]:
                    temp_result.append(data + [c])
        
        current_result = temp_result
    
    return current_result