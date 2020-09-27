import pymongo
from datetime import datetime
import time

myClient = pymongo.MongoClient('mongodb://localhost:27017/')
dblist = myClient.list_database_names()
# dblist = myclient.database_names() 
if "ttd" in dblist:
    print("数据库已存在！")

events = []
firms = []
human = []
institutions = []
file = open('referenceList/eventList.txt', 'r')
for line in file.readlines():
    events.append(line.strip())
file.close()
file = open('referenceList/firmList.txt', 'r')
for line in file.readlines():
    firms.append(line.strip())
file.close()
file = open('referenceList/humanList.txt', 'r')
for line in file.readlines():
    human.append(line.strip())
file.close()
file = open('referenceList/institutionList.txt', 'r')
for line in file.readlines():
    institutions.append(line.strip())
file.close()
tagList = [events, firms, human, institutions]

mydb = myClient["ttd"]
 
mycol = mydb["news"]

cList = mydb.list_collection_names()
if 'taggedNews' in cList:
    print('taggedNews exsited')
else:
    taggedNews = mydb['taggedNews']
    print('Create taggedNews')

temp = mycol.find()
for document in temp:
    firmsTag = []
    for firm in firms:
        if firm in document['content']:
            firmsTag.append(firm)
    document['firmsTag'] = firmsTag
    if len(firmsTag) > 0:
        taggedNews.insert_one(document)
    
