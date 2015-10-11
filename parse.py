import xml.etree.ElementTree as etree
import sqlite3 as sql
import time

def createDatabase(dbname):
    conn = sql.connect(dbname)
    cursor = conn.cursor()
    create = "CREATE TABLE Pages (ID integer, Title text, Contents text, Redirects text, PRIMARY KEY (ID))"
    cursor.execute(create)
    conn.commit()
    return conn

def insertValues(cursor, ID, title, contents):
    command = "INSERT INTO Pages VALUES (?, ?, ?, '')"
    args = (ID, title, contents)
    cursor.execute(command, args)

"""
Store data into the database.
"""
def parseData():
    fName = "wikiDBxml"
    dbName = "wikiDB.db"
    isDB = False
    if not isDB:
        conn = createDatabase(dbName)
    else:
        conn = sqlite.connect(dbName)

    events = ("start", "end")
    tree = etree.iterparse(fName, events)

    #need to get the root to clear it time to time because of memory
    event, root = tree.next()

    startT = time.time()
    t = startT
    isCollecting, ID, title, text = False, None, None, None
    count, countNotCollected, countRedirects = 0, 0, 0
    #count = overall number of articles
    #counNotCollected = not collected articles because incomplete
    #countRedirects = number of redirecting articles
    for event, elem in tree:
        if count > 0:
            break
        if elem.tag[-4:] == "page" and event == "start":
            count += 1
            isCollecting = True
            #print elem.text.encode('utf-8')
        if elem.tag[-6:] == "format" and event == "end":
            if len(elem.attrib) != 0:
                print elem.attrib
            if type(elem.text) is str:
                print elem.text.encode('utf-8')
        if time.time() - t > 60:
            print str((time.time() - startT) // 60) + " min"
            print "articles: " + str(count)
            t = time.time()
            root.clear() #clear the root
        #don't want to keep the element in memory
        elem.clear()
    print "Number of articles: " + str(count)
    print "Total time: " + str((time.time() - startT)) + " seconds"

createDatabase()

