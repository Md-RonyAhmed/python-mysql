import json
import mysql.connector

# Read data from the JSON file
with open('data.json', 'r') as file:
    brokers_out = json.load(file)  # encoding

# Convert the Python dictionary to a JSON string
data_out = json.dumps(brokers_out) # decoding

# Connect to MySQL Server 
connection = mysql.connector.connect(
    host='sql6.freemysqlhosting.net',    
    user='sql6634527',                  
    password='eW7ViMdbkk',          
    database='sql6634527'         
)

# Create a cursor
cursor = connection.cursor()

# Drop the 'brokers' table if it exists
drop_table_query = 'DROP TABLE IF EXISTS brokers'

cursor.execute(drop_table_query)

# Create the 'brokers' table
create_table_query = '''
CREATE TABLE brokers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    broker_name VARCHAR(50) NOT NULL,
    broker_ip VARCHAR(50) NOT NULL
)
'''

cursor.execute(create_table_query)

# Insert the data into the table
insert_query = 'INSERT INTO brokers (broker_name, broker_ip) VALUES (%s, %s)'
for broker_name, broker_ip in brokers_out.items():
    cursor.execute(insert_query, (broker_name, broker_ip))

# Commit the changes
connection.commit()

print("Table created and data imported successfully!")

# Execute a SELECT query to fetch data from the table
select_query = 'SELECT * FROM brokers'
cursor.execute(select_query)

# Fetch all rows from the cursor
rows = cursor.fetchall()

# Print the data
print("Data from the 'brokers' table:")
for row in rows:
    print(f"ID: {row[0]}, Broker Name: {row[1]}, Broker IP: {row[2]}")

# Close the cursor and connection
cursor.close()
connection.close()