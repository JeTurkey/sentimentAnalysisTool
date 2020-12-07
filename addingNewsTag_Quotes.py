import mysql.connector
import re

mydb  = mysql.connector.connect(
    host='rm-bp11g1acc24v9f69t1o.mysql.rds.aliyuncs.com',
    user='rayshi',
    password='Rayshi1994!',
    database='ttd',
    auth_plugin='mysql_native_password'
)

mycursor = mydb.cursor()

mycursor.execute('SELECT * FROM ttd.news')

result = mycursor.fetchall()

for line in result:
    ptn = r'《.+?》'
    rst = re.findall(ptn,line[4])
    policy_list = ''
    for item in rst:
        if len(item) > 6 and item not in policy_list:
            policy_list += item + ','
    if len(policy_list) > 1:
        try:
            sql = 'INSERT INTO tag_quotes_news VALUES (%s, %s);'
            val = (line[0], policy_list[:-1])
            mycursor.execute(sql, val)
            mydb.commit()
        except:
            pass


print('All done')
mydb.close()