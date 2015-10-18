import xml.etree.ElementTree as etree
import sqlite3 as sql
import time
import cPickle
import zlib

class Parser:
    """
    Go through the XML data piecewise and store it into the database.
    """
    def parseData(self):
        fName = "wikiDBxml"
        dbName = "wikiDBsql"
        #dbName = "dummy.db"
        #self.conn = self.createDatabase(dbName)
        self.conn = sql.connect(dbName)

        events = ("start", "end")
        tree = etree.iterparse(fName, events)

        #need to get the root to clear it time to time because of memory
        event, self.root = tree.next()

        startT = time.time()
        self.cursor = self.conn.cursor()
        self.collector = DataCollector()

        for event, elem in tree:
            try:
                self.oneStep(event, elem)
            except Exception as e:
                print "Error! Skipping the entry."
                print "Reason: " + str(e)
                print "Current title: ", self.collector.title
                self.collector.reset()
        self.conn.commit()

        print "Time it took: " + str(time.time() - startT) + " seconds."

        self.collector.reportStats()
        print "Data collected. Inserting links..."
        for ID in self.collector.links:
            links = "::".join(self.collector.links[ID])
            args = (links.decode("utf-8"), ID)
            self.cursor.execute("UPDATE Pages SET Redirections = ? WHERE ID = ?", args)
        self.conn.commit()
        print "Links inserted"

        self.conn.close()

    def oneStep(self, event, elem):
        self.collector.countTotal += 1
        self.collector.countTags += 1
        try:
            self.collector.updateValues(self, event, elem)
        except Exception as e:
            print "\n"
            print e
            self.collector.reset()
        if self.collector.countTags > 100000 and event == "end":
            self.collector.countTags = 0
            self.conn.commit()
            self.root.clear() #clear the root
        #don't want to keep the element in memory
        if event == "end":
            elem.clear()

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


"""Collect one row of the data at a time and
keep track of the collection state."""
class DataCollector:
    def __init__(self):
        self.title = None
        self.text = None
        self.redirTo = None
        #is the currect article a redirecting article?
        self.isRedir = False
        #are we at the state of collecting values for the article?
        #serves as a safeguard against unexpected situations
        self.isCollecting = False
        #number of collected articles
        self.countCollected = 0
        #number of redirecting articles
        self.countRedirects = 0
        #total number of tags
        self.countTotal = 0
        #total number of articles
        self.countPages = 0
        self.countTags = 0
        self.links = dict()

    """After we have collected a row, we reset all state variables"""
    def reset(self):
        self.title, self.text = None, None
        self.isCollecting = False
        self.isRedir = False

    """Update page values. We do not assume the XML tags for each
    page are ordered the same way."""
    def updateValues(self, parser, event, elem):
        #registered start of a page
        if elem.tag[-4:] == "page" and event == "start":
            self.isCollecting = True
            self.countPages += 1

        #this page is a redirecting page
        elif elem.tag[-8:] == "redirect" and self.isCollecting:
           self.redirTo = self.countPages
           self.isRedir = True

        #collect a title
        elif elem.tag[-5:] == "title" and self.isCollecting and event == "end":
            self.title = elem.text.encode('utf-8')

        #collect the article body
        elif elem.tag[-4:] == "text" and self.isCollecting and event == "end":
            if elem.text is None:
                self.text = u''
            else:
                self.text = elem.text.encode('utf-8')

        elif elem.tag[-5:] == "model" and self.isCollecting and event == "end":
            self.model = elem.text.encode('utf-8')
        elif elem.tag[-6:] == "format" and self.isCollecting and event == "end":
            self.form = elem.text.encode('utf-8')

        #end of the page, insert into DB
        elif elem.tag[-4:] == "page" and event == "end":
            self.insertValues(parser)

    def insertValues(self, parser):
        if self.isRedir:
            self.countRedirects += 1
            if self.redirTo not in self.links:
                self.links[self.redirTo] = [self.title]
            else:
                self.links[self.redirTo].append(self.title)
        else:
            try:
                parser.insertRow(self.countPages, self.title, self.text)
                self.countCollected += 1
            except Exception as e:
                print "Unable to insert row."
                print "Reason: " + str(e)
                print "Title: ", self.title
        self.reset()

    def reportStats(self):
        print "collected: " + str(self.countCollected)
        print "redirects: " + str(self.countRedirects)
        print "total count: " + str(self.countPages)

if __name__ == "__main__":
    parser = Parser()
    parser.parseData()

