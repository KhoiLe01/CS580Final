# Problem 2 implementation:
def construct_hashmap_problem2(arr, index):
    """
    Docstring for construct_hashmap_problem2
    
    :param arr: a 2D array
    :param index: the index to be used as key. Only 0 or 1.
    :return: a hashmap where the keys are the elements at the given index.
    """
    h = {}
    for i in range(len(arr)):
        cur_element = arr[i]
        key = cur_element[index]
        val = cur_element[1 - index]
        if key not in h:
            h[key] = [val]
        else:
            h[key].append(val)
    return h

def remove_dangling_tuple(db):
    """
    Docstring for remove_dangling_tuple
    
    :param db: a 3D array. Each element is a 2D array representing a table.
    """
    h_bottom_up = {i: construct_hashmap_problem2(db[i], 1) for i in range(len(db))}
    h_top_down = {i: construct_hashmap_problem2(db[i], 0) for i in range(len(db))}
    
    # Bottom up pass
    for i in range(len(db)-2, -1, -1):
        for j in range(len(db[i])):
            cur_ai1 = db[i][j][0]
            cur_ai2 = db[i][j][1]
            if h_top_down[i+1].get(cur_ai2) is None:
                h_bottom_up[i][cur_ai2].remove(cur_ai1)
                if len(h_bottom_up[i][cur_ai2]) == 0:
                    del h_bottom_up[i][cur_ai2]

    # Top down pass is not necessary because when we obtain the result by going top down,
    # the dangling tuples would not be included anyway. The top down pass is automatically
    # handled in get_result function.
    # for i in range(1, len(db)):
    #     for j in range(len(db[i])):
    #         cur_ai1 = db[i][j][0]
    #         cur_ai2 = db[i][j][1]
    #         if h_bottom_up[i-1].get(cur_ai1) is None:
    #             h_top_down[i][cur_ai1].remove(cur_ai2)
    #             if len(h_top_down[i][cur_ai1]) == 0:
    #                 del h_top_down[i][cur_ai1]
                
    return h_bottom_up, h_top_down

def get_result(h_top_down):
    """
    Docstring for get_result
    
    :param h_top_down: a hashmap from remove_dangling_tuple function. This is used by the DFS to get the result of the query.
    """
    result = []
    def dfs(i, current_tuple, val):
        # If we have reached the end, append the current tuple to result
        if i == len(h_top_down):
            for j in current_tuple:
                result.append(j + val)
            return
        
        # If we are at the first relation, we need to initialize the current_tuple
        # and call dfs for the next relation
        if i == 0:
            for key in h_top_down[i]:
                for tup in h_top_down[i][key]:
                    dfs(i+1, [[key]], [tup])
                    
        # Else, append to existing values to current_tuple and call dfs for the next relation
        else:
            for key in val:
                if key in h_top_down[i]:
                    for tup in h_top_down[i][key]:
                        dfs(i+1, [i + val for i in current_tuple], [tup])                    
    
    dfs(0, [], [])
    return result

#############################################
# Test case
# R1 = [[1, 10], [2, 20], [3, 30]]
# R2 = [[10, 100], [20, 200], [99, 999], [30, 200]]   # (99,999) is dangling
# R3 = [[100, 1000], [200, 2000]]

# relations = [R1, R2, R3]

# h1, h2 = remove_dangling_tuple(relations)
# result = get_result(h2)
# for r in result:
#     print(r)
#############################################