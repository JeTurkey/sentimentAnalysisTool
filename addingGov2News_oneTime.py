import mysql.connector

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

print('All news has been fetched')

sql = 'SELECT * FROM ttd.gov_dept'

mycursor.execute(sql)

gov_dept = mycursor.fetchall()

print('All Government has been fetched')

# Turn government department into dict
ind_to_dept = {}
dept_to_nick = {}
for line in gov_dept:
    ind_to_dept[line[1]] = line[0]
    dept_to_nick[line[1]] = line[2].split(",")

# building gov_news talbe

for line in result:
    for nick in dept_to_nick:
        for name in dept_to_nick[nick]:
            if str(name) in line[1] or str(name) in line[4]:
                try:
                    sql = "INSERT INTO ttd.gov_news (gov_dept_id, news_id, news_date) VALUES (%s, %s, %s)"
                    val = (ind_to_dept[nick], line[0], line[3])
                    mycursor.execute(sql, val)
                    mydb.commit()
                    break
                except:
                    pass
                
print('All done')
mydb.close()
