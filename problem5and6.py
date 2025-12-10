import random, time
# Import the algorithm implementations from problem 2 and 3
from problem2 import remove_dangling_tuple, get_result
from problem3 import problem3_algo
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Problem 5 implementation: Set up database to test the efficiency of the algorithms
R1 = []

for i in range(1, 1001):
    R1.append([i, 5])

for i in range(1001, 2001):
    R1.append([i, 7])

R1.append([2001, 2002])
random.shuffle(R1)


R2 = []
for i in range(1, 1001):
    R2.append([5, i])

for i in range(1001, 2001):
    R2.append([7, i])
R2.append([2002, 8])
random.shuffle(R2)

R3 = []
for i in range(2000):
    R3.append([random.randint(2002, 3000), random.randint(1, 3000)])

R3.append([8, 30])
random.shuffle(R3)

db = [R1, R2, R3]

# Execute problem 2's implementation
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

# Execute problem 3's implementation using hashmap
start_time = time.perf_counter()
result = problem3_algo(db)
print("Results from Problem 3's implementation:")
if not result:
    print("No result found.")
else:
    for r in result:
        print(r)
end_time = time.perf_counter()
print(f"Execution time for problem 3's implementation: {end_time - start_time} seconds")


######################################################
# Problem 6 implementation: Set up database in MySQL to test the efficiency of SQL query
mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password=os.getenv("PASSWORDSQL"),
      database="cs580final"
    )

mycursor = mydb.cursor()

# Create tables. Commented out to avoid re-creation errors.
# mycursor.execute("CREATE TABLE R1 (a1 INT NOT NULL, a2 INT NOT NULL);")
# mycursor.execute("CREATE TABLE R2 (a2 INT NOT NULL, a3 INT NOT NULL);")
# mycursor.execute("CREATE TABLE R3 (a3 INT NOT NULL, a4 INT NOT NULL);")

# Insert data into R1. Commented out to avoid duplicate entries.
empty_query = "TRUNCATE TABLE R1;"
insert_query = "INSERT INTO R1 (a1, a2) VALUES (%s, %s)"

mycursor.execute(empty_query)
mydb.commit()

mycursor.executemany(insert_query, R1)
mydb.commit()

# Insert data into R2.
empty_query = "TRUNCATE TABLE R2;"
insert_query = "INSERT INTO R2 (a2, a3) VALUES (%s, %s)"

mycursor.execute(empty_query)
mydb.commit()

mycursor.executemany(insert_query, R2)
mydb.commit()

# Insert data into R3.
empty_query = "TRUNCATE TABLE R3;"
insert_query = "INSERT INTO R3 (a3, a4) VALUES (%s, %s)"

mycursor.execute(empty_query)
mydb.commit()

mycursor.executemany(insert_query, R3)
mydb.commit()

# Execute SQL query and measure execution time
start_time = time.perf_counter()
mycursor.execute("SELECT r1.a1, r2.a2, r3.a3, r3.a4 FROM r1 join r2 on r1.a2 = r2.a2 join r3 on r2.a3 = r3.a3;")
end_time = time.perf_counter()
print(f"Execution time for MySQL: {end_time - start_time} seconds")
myresult = mycursor.fetchall()
print("Results from MySQL:")
if not myresult:
    print("No result found.")
else:
    for r in myresult:
        print(r)
        
# Get and print the query execution plan
mycursor.execute("EXPLAIN FORMAT=TREE SELECT r1.a1, r2.a2, r3.a3, r3.a4 FROM r1 join r2 on r1.a2 = r2.a2 join r3 on r2.a3 = r3.a3;")
myresult = mycursor.fetchall()
print("Query Execution Plan from MySQL:")
for r in myresult:
    print(r[0].replace('\\n', '\n'))