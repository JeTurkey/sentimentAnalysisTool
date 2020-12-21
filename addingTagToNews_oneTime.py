import mysql.connector

mydb  = mysql.connector.connect(
    host='rm-bp11g1acc24v9f69t1o.mysql.rds.aliyuncs.com',
    user='rayshi',
    password='Rayshi1994!',
    database='ttd',
    auth_plugin='mysql_native_password'
)


# mydb  = mysql.connector.connect(
#     host='localhost',
#     user='root',
#     password='Rayshi1994!',
#     database='ttd'
# )

mycursor = mydb.cursor()

mycursor.execute('SELECT * FROM ttd.news')

result = mycursor.fetchall()

sql = 'SELECT * FROM ttd.gov_dept'

mycursor.execute(sql)

gov_dept = mycursor.fetchall()
print(gov_dept)

# Turn government department into dict
ind_to_dept = {}
dept_to_nick = {}
for line in gov_dept:
    ind_to_dept[line[1]] = line[0]
    dept_to_nick[line[1]] = line[2].split(",")



# building gov_news talbe

for line in result:
    gov_tag = ''
    for nick in dept_to_nick:
        for name in dept_to_nick[nick]:
            if name in line[1] or name in line[4]:
                gov_tag += nick + ','
                break
    try:
        sql = 'UPDATE ttd.news SET gov_tag = %s WHERE news_id = %s'
        val = (gov_tag[:-1], line[0])
        mycursor.execute(sql, val)
        mydb.commit()
    except:
        pass

print('Government Done')

# 公司
sql = 'SELECT * FROM ttd.company'

mycursor.execute(sql)

gov_dept = mycursor.fetchall()
print(gov_dept)

# Turn government department into dict
ind_to_dept = {}
dept_to_nick = {}
for line in gov_dept:
    ind_to_dept[line[1]] = line[0]
    dept_to_nick[line[1]] = line[2].split(",")

# building gov_news talbe

for line in result:
    gov_tag = ''
    for nick in dept_to_nick:
        for name in dept_to_nick[nick]:
            if name in line[1] or name in line[4]:
                gov_tag += nick + ','
                break
    try:
        sql = 'UPDATE ttd.news SET com_tag = %s WHERE news_id = %s'
        val = (gov_tag[:-1], line[0])
        mycursor.execute(sql, val)
        mydb.commit()
    except:
        pass

print('Company Done')
                
print('All done')
mydb.close()