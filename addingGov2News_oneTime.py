import mysql.connector

mydb  = mysql.connector.connect(
    host='rm-bp11g1acc24v9f69t1o.mysql.rds.aliyuncs.com',
    username='rayshi',
    password='Rayshi1994',
    database='ttd',
    auth_plugin='mysql_native_password'
)

mycursor = mydb.cursor()

mycursor.execute('SELECT * FROM ttd.news')

result = mycursor.fetchall()

sql = 'SELECT * FROM government_dept'

mycursor.execute(sql)

gov_dept = mycursor.fetchall()

# Turn government department into dict
ind_to_dept = {}
dept_to_nick = {}
for line in gov_dept:
    ind_to_dept[line[1]] = line[0]
    dept_to_nick[line[1]] = line[2].split(",")

# building gov_news talbe

for line in result[:10]:
    for nick in dept_to_nick:
        for name in dept_to_nick[nick]:
            if name in line[1] or name in line[4]:
                try:
                    sql = "INSERT INTO gov_news VALUES (%s, %s)"
                    val = (ind_to_dept[nick], line[0])
                    mycursor.execute(sql, val)
                    mydb.commit()
                    
                except:
                    pass
                
print('All done')
mydb.close()