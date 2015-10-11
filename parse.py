import xml.etree.ElementTree as etree

fName = "wikiDB"

count = 0

#print first 100 titles
for event, elem in etree.iterparse(fName):
    if count > 100:
        break
    if elem.tag[-5:] == "title":
        count += 1
        print elem.text.encode('utf-8')
