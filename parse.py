import xml.etree.ElementTree as etree
import time

"""
Counts the number of articles.
"""
count = 0
fName = "wikiDBxml"
startT = time.time()
t = startT

for event, elem in etree.iterparse(fName):
    if count > 100:
        break
    if elem.tag[-5:] == "title":
        count += 1
        #print title (need Unicode)
        #print elem.text.encode('utf-8')
    if time.time() - t > 60:
        print str((time.time() - startT) // 60) + " min"
        print "articles: " + count
        t = time.time()

print "Number of articles: " + count
