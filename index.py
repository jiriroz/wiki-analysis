import sqlite3

con = sqlite3.connect("wikiDBsql")
cur = con.cursor()

cur.execute("CREATE UNIQUE INDEX TitleIndex ON Pages (Title)")

con.commit()
con.close()
