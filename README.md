import cx_Oracle

# Replace 'username', 'password', 'hostname', 'port', and 'service_name' with your actual database details
username = 'your_username'
password = 'your_password'
dsn = 'hostname:port/service_name'
port = 1521
service_name = 'your_service_name'

# Construct the connection string
dsn_tns = cx_Oracle.makedsn('hostname', port, service_name=service_name)

# Establish the connection
connection = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)

# Create a cursor
cursor = connection.cursor()

# Execute a query
cursor.execute('SELECT * FROM your_table')

# Fetch the results
for row in cursor:
    print(row)

# Close the cursor and connection
cursor.close()
connection.close()


import cx_Oracle

# Assuming connection is already established
# connection = cx_Oracle.connect(user, password, dsn)

# Create a cursor
cursor = connection.cursor()

# Execute a query to get all tables accessible by the current user
cursor.execute("SELECT table_name FROM ALL_TABLES")

# Fetch and print the results
print("Accessible Tables:")
for row in cursor:
    print(row[0])

# Close the cursor
cursor.close()
