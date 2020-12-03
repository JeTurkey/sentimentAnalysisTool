import schedule
import time
import mysql.connector

def tagGov():
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

    sql = 'SELECT * FROM ttd.gov_dept'
    mycursor.execute(sql)
    gov_dept = mycursor.fetchall()

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
                if name in line[1] or name in line[4]:
                    try:
                        sql = "INSERT INTO gov_news VALUES (%s, %s)"
                        val = (ind_to_dept[nick], line[0])
                        mycursor.execute(sql, val)
                        mydb.commit()
                        break
                    except:
                        pass
                    
    print('All done')
    mydb.close()


def tagCom():
    mydb  = mysql.connector.connect(
        host='rm-bp11g1acc24v9f69t1o.mysql.rds.aliyuncs.com',
        user='rayshi',
        password='Rayshi1994!',
        database='ttd',
        auth_plugin='mysql_native_password'
        )

    mycursor = mydb.cursor()
    mycursor.execute('SELECT * FROM ttd.news ORDER BY news_id DESC LIMIT 30;')
    result = mycursor.fetchall()

    sql = 'SELECT * FROM ttd.company'
    mycursor.execute(sql)
    gov_dept = mycursor.fetchall()

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
                if name in line[1] or name in line[4]:
                    try:
                        sql = "INSERT INTO ttd.com_news VALUES (%s, %s)"
                        val = (ind_to_dept[nick], line[0])
                        mycursor.execute(sql, val)
                        mydb.commit()
                        break
                    except:
                        pass
                    
    print('All done')
    mydb.close()


def tagging():
    tagGov()
    print('tag Government Done')
    tagCom()
    print('tag Company Done')




schedule.every().hour.do(tagging)


if __name__ == "__main__":
    print('Running...')
    while True:
        print(time.localtime())
        schedule.run_pending()
        time.sleep(3600)
    