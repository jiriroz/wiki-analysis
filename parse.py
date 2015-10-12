import xml.etree.ElementTree as etree
import sqlite3 as sql
import time

"""Collect one row of the data at a time and
keep track of the collection state."""
class DataCollector:
    def __init__(self):
        self.title = None
        self.text = None
        self.redir = None
        #is the currect article a redirecting article?
        self.isRedir = False
        #are we at the state of collecting values for the article?
        #serves as a safeguard against unexpected situations
        self.isCollecting = False
        #number of collected articles
        self.countCollected = 0
        #not collected articles because incomplete
        self.countNotCollected = 0
        #number of redirecting articles
        self.countRedirects = 0
        #total number of articles
        self.countTotal = 0
        #last element collected
        self.last = "No element"

    """Check whether all values of a row are populated"""
    def allCollected(self):
        return None not in [self.title, self.text]

    """After we have collected a row, we reset all state variables"""
    def reset(self):
        self.title, self.text = None, None
        self.isCollecting = False
        self.isRedir = False

    """Update page values. We do not assume the XML tags for each
    page are ordered the same way."""
    def updateValues(self, event, elem):
        #registered start of a page
        if elem.tag[-4:] == "page" and event == "start":
            self.isCollecting = True
            self.countTotal += 1

        #this page is a redirecting page
        if elem.tag[-8:] == "redirect" and self.isCollecting:
           self.redir = elem.attrib["title"].encode('utf-8')
           self.isRedir = True

        #collect a title
        if elem.tag[-5:] == "title" and self.isCollecting and event == "end":
            self.title = elem.text.encode('utf-8')

        #collect the article body
        if elem.tag[-4:] == "text" and self.isCollecting and event == "end":
            self.text = elem.text.encode('utf-8')

    def insertValues(self, parser):
        if self.isRedir:
            self.countRedirects += 1
            #right now skipping the redirecting articles
            self.last = self.title
        elif self.allCollected():
            #count serves as an ID
            try:
                parser.insertRow(self.countCollected, self.title, self.text)
                self.countCollected += 1
                self.last = self.title
            except Exception as e:
                print "Unable to insert row."
                print "Reason: " + e
                print "Title: ", self.title
                self.countNotCollected += 1
        else:
            self.countNotCollected += 1
        self.reset()

    def reportStats(self):
        print "collected: " + str(self.countCollected)
        print "unable to collect: " + str(self.countNotCollected)
        print "redirects: " + str(self.countRedirects)
        print "total count: " + str(self.countTotal)


class Parser:
    def __init__(self):
        pass

    def createDatabase(self, dbname):
        conn = sql.connect(dbname)
        cursor = conn.cursor()
        create = "CREATE TABLE Pages (ID integer, Title text, Contents text, Redirections text, PRIMARY KEY (ID))"
        cursor.execute(create)
        conn.commit()
        return conn
    
    def insertRow(self, ID, title, contents):
        command = "INSERT INTO Pages VALUES (?, ?, ?, '')"
        args = (ID, title.decode('utf-8'), contents.decode('utf-8'))
        self.cursor.execute(command, args)

    def oneStep(self, event, elem):
        self.collector.updateValues(event, elem)
        if elem.tag[-4:] == "page" and event == "end":
            self.collector.insertValues(self)
        if self.collector.countTotal % 10000 == 0:
            self.conn.commit()
            self.root.clear() #clear the root
        #don't want to keep the element in memory
        if event == "end":
            elem.clear()
    
    """
    Go through the XML data piecewise and store it into the database.
    """
    def parseData(self):
        fName = "wikiDBxml"
        dbName = "wikiDB10E5.db"
        self.conn = self.createDatabase(dbName)
        #self.conn = sql.connect(dbName)
    
        events = ("start", "end")
        tree = etree.iterparse(fName, events)
    
        #need to get the root to clear it time to time because of memory
        event, self.root = tree.next()
    
        startT = time.time()
        self.cursor = self.conn.cursor()
        self.collector = DataCollector()
    
        for event, elem in tree:
            if self.collector.countTotal > 100000:
                break
            try:
                self.oneStep(event, elem)
            except Exception as e:
                print "Error! Skipping the entry."
                print "Reason: " + e
                print "Last page collected: " + self.collector.last
                print "Current title: ", self.collector.title
                self.collector.reset()
                continue

        self.conn.commit()
        self.conn.close()
        self.collector.reportStats()
        print "Total time: " + str(time.time() - startT) + " sec."
    
if __name__ == "__main__":
    parser = Parser()
    parser.parseData()

