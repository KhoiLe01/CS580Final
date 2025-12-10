# Problem 3 implementation brute force: This implementation is directly join the line query from left to right by nested loops.
# Not used in the report. Just used for personal comparison
def inner_join_left_to_right_brute_force(db):
    cur_result = db[0]
    for i in range(1, len(db)):
        temp_result = []
        cur_db = db[i]
        for j in cur_result:
            for k in cur_db:
                if k[0] == j[-1]:
                    temp_result.append(j + [k[1]])
        cur_result = temp_result
    return cur_result