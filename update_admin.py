import sqlite3

# Connect to your SQLite database file
conn = sqlite3.connect('stock_simulator.db')
cursor = conn.cursor()

# Update the user "Tim Brockman" to set is_admin to 1 (True)
cursor.execute("UPDATE user SET is_admin = 1 WHERE username = 'Tim Brockman'")

conn.commit()
conn.close()

print("Tim Brockman has been designated as an admin.")
