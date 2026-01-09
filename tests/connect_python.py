import oracledb

# Test connection to Oracle database
# conn = oracledb.connect(
#     user="read_user",
#     password="ThisIsASecret123",
#     dsn="localhost/freepdb1"
# )

conn = oracledb.connect(
    user="admin",
    password="ThisIsASecret123",
    dsn="test-oracle-19c.ctgcsik2itm5.ap-southeast-1.rds.amazonaws.com:1521/pdb1"
)

cursor = conn.cursor()
cursor.execute("SELECT 'Hello, Oracle!' FROM dual")
for row in cursor:
    print(row[0])