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

def insertValues(ID, title, contents, cursor):
    command = "INSERT INTO Pages VALUES (?, ?, ?, '')"
    args = (ID, title, contents)
    cursor.execute(command, args)

"""
Store data into the database.
"""
def parseData():
    fName = "wikiDBxml"
    #fName = "/media/jiri/26AC04CEAC049B07/Users/Jirkaroz/Wikienwiki-20150901-pages-articles.xml"
    dbName = "wikiDB.db"
    isDB = True
    if not isDB:
        conn = createDatabase(dbName)
    else:
        conn = sql.connect(dbName)

    events = ("start", "end")
    tree = etree.iterparse(fName, events)
    print tree

    #need to get the root to clear it time to time because of memory
    event, root = tree.next()

    startT = time.time()
    t = startT
    ID, title, text = None, None, None
    isCollecting, collectID = False, False
    count, countNotCollected, countRedirects = 0, 0, 0
    cursor = conn.cursor()
    #count = overall number of articles
    #counNotCollected = not collected articles because incomplete
    #countRedirects = number of redirecting articles
    for event, elem in tree:
        if count > 100:
            break
        if elem.tag[-4:] == "page" and event == "start":
            count += 1
            isCollecting = True
            collectID = True
            #print elem.text.encode('utf-8')
        if elem.tag[-5:] == "title" and isCollecting and event == "end":
            title = elem.text.encode('utf-8')
        if elem.tag[-4:] == "text" and isCollecting and event == "end":
            text = elem.text.encode('utf-8')
        if elem.tag[-7:] == "revision" and event == "start":
            collectID = False
        if elem.tag[-7:] == "revision" and event == "end:":
            collectID = True
        if elem.tag[-2:] == "id" and event == "end" and collectID:
            ID = int(elem.text)
        if elem.tag[-4:] == "page" and event == "end":
            if None not in [ID, title, text]:
                print ID
                print title
                print text[:50]
                #insertValues(ID, title, text, cursor)
            else:
                print "can't insert"
                countNotCollected += 1
            ID, title, text = None, None, None
            
        if time.time() - t > 60:
            print str((time.time() - startT) // 60) + " min"
            print "articles: " + str(count)
            t = time.time()
            conn.commit()
            root.clear() #clear the root
        #don't want to keep the element in memory
        elem.clear()
    conn.commit()
    conn.close()
    print "Number of articles: " + str(count)
    print "Total time: " + str((time.time() - startT)) + " seconds"

if __name__ == "__main__":
    parseData()

