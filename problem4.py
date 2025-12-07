import random, time
# Import the algorithm implementations from problem 2 and 3
from problem2 import remove_dangling_tuple, get_result
from problem3_hashmap import inner_join_left_to_right_efficient
from problem3_bruteforce import inner_join_left_to_right_brute_force

# Problem 4: Set up test database
R1 = []
for i in range(1, 101):
    R1.append([i, random.randint(1, 5000)])

R2 = []
for i in range(1, 101):
    R2.append([random.randint(1, 5000), i])
    
R3 = []
for i in range(1, 101):
    R3.append([i, i])
    
db = [R1, R2, R3]

start_time = time.perf_counter()
h1, h2 = remove_dangling_tuple(db)
result = get_result(h2)
print("Results from Problem 2's implementation:")
if not result:
    print("No result found.")
else:
    for r in result:
        print(r)
end_time = time.perf_counter()
print(f"Execution time for problem 2's implementation: {end_time - start_time} seconds")

start_time = time.perf_counter()
result = inner_join_left_to_right_efficient(db)
print("Results from Problem 3's implementation using hashmap:")
if not result:
    print("No result found.")
else:
    for r in result:
        print(r)
end_time = time.perf_counter()
print(f"Execution time for problem 3's implementation using hashmap: {end_time - start_time} seconds")

start_time = time.perf_counter()
result = inner_join_left_to_right_brute_force(db)
print("Results from Problem 3's implementation using bruteforce:")
if not result:
    print("No result found.")
else:
    for r in result:
        print(r)
end_time = time.perf_counter()
print(f"Execution time for problem 3's implementation using bruteforce: {end_time - start_time} seconds")
