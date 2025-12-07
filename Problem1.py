def hash_join(R1, R2):
    # Build hash table on R2(B, C)
    h = {}
    for (b, c) in R2:
        if b not in h:
            h[b] = []
        h[b].append((b, c))

    # Probe using R1(A, B)
    result = []
    for (a, b) in R1:
        if b in h:
            for (_, c) in h[b]:
                result.append((a, b, c))

    return result


# 10-tuple dataset
R1 = [(1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)]
R2 = [(10,101),(20,102),(30,103),(40,104),(50,105),(60,106),(70,107),(80,108),(90,109),(100,110)]

# Test the join
output = hash_join(R1, R2)
print("Join Output:")
for row in output:
    print(row)
