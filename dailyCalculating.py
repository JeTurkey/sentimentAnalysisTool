import pymongo
import time
from datetime import datetime
import pandas as pd

myClient = pymongo.MongoClient('mongodb://localhost:27017/')
dblist = myClient.list_database_names()
# Test if db existed
if 'ttd' in dblist:
    print('DB existed')
mydb = myClient['ttd']
cList = mydb.list_collection_names()

# 标记过的新闻库
if 'taggedNews' in cList:
    print('Collection existed')
else:
    print('Create collection')
    
taggedNews = mydb['taggedNews']

# 得分库

if 'scorebases' in cList:
    print('Collection existed')
else:
    print('Create collection')

scoreBase = mydb['scorebases']

def initialization():
    
    # institution weight
    institutionWeight = {}
    institutionFile = open('referenceList/institutionWeight.txt', 'r')
    for line in institutionFile.readlines():
        ins, weight = line.split(': ')
        institutionWeight[str(ins.strip())] = float(weight.strip())
    institutionFile.close()
        
    # source weight    
    sourceWeight = {}
    sourceFile = open('referenceList/sourceWeight.txt', 'r')
    for line in sourceFile.readlines():
        source, weight = line.split(': ')
        sourceWeight[str(source.strip())] = float(weight.strip())
    sourceFile.close()
    
    # positive sentiment vocabs
    positiveSentimentVocabs = {}
    positiveSentimentVocabsFile = open('referenceList/positiveSentimentVocabs.txt', 'r')
    for line in positiveSentimentVocabsFile.readlines():
        vocab, weight = line.split(': ')
        positiveSentimentVocabs[str(vocab.strip())] = float(weight.strip())
    positiveSentimentVocabsFile.close()
    
    # negative sentiment vocabs
    negativeSentimentVocabs = {}
    negativeSentimentVocabsFile = open('referenceList/negativeSentimentVocabs.txt', 'r')
    for line in negativeSentimentVocabsFile.readlines():
        vocab, weight = line.split(': ')
        negativeSentimentVocabs[str(vocab.strip())] = float(weight.strip())
    negativeSentimentVocabsFile.close()
        
    return institutionWeight, sourceWeight, positiveSentimentVocabs, negativeSentimentVocabs

firms = []
file = open('referenceList/firmList.txt', 'r')
for line in file.readlines():
    firms.append(line.strip())
file.close()




def analysis(target, content):
    institutionWeight, sourceWeight, positive_words, negative_words = initialization()
    currentScore = [0]
    
    for sentence in content.split('。'):
        for word in positive_words:
            for institution in institutionWeight:
                if word in sentence and institution in sentence:
                    currentScore.append(currentScore[-1] + institutionWeight[institution] * positive_words[word])
        
        for word in negative_words:
            for institution in institutionWeight:
                if word in sentence and institution in sentence:
                    currentScore.append(currentScore[-1] + institutionWeight[institution] * negative_words[word])
                    
    return currentScore

def SAT(firms, givenDate):

    data = taggedNews.find({'dateAdded': givenDate})
    
    firmScore = pd.DataFrame(columns = ['firmName', 'firmScore', 'scoreDate'])
    
    for line in data:
        for firm in firms:
            if firm in line['firmsTag']:
                # 触发分析
               
                score = analysis(firm, line['content'])
                firmScore = firmScore.append({'firmName': firm, 'firmScore': round(float(score[-1]),2), 'scoreDate': givenDate}, ignore_index = True)
                
    result = firmScore.groupby('firmName').sum()
    
    result = result.reset_index()
    result['firmScore'] = round(result['firmScore'], 2)
    for firm in firms:
        if firm not in list(result['firmName']):
            
            result = result.append({'firmName': firm,'firmScore': round(float(0), 2)}, ignore_index = True)
    return result

date = set()
for line in taggedNews.find():
    date.add(line['dateAdded'])

for time in date:
    current = SAT(firms, time)
    for i in range(len(current)):
        scoreBase.insert_one({'firmName': current.iloc[i]['firmName'],
                              'firmScore': current.iloc[i]['firmScore'],
                              'scoreDate': time})