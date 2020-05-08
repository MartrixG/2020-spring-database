import pymysql

# 数据库参数
config = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'root',
          'database':'lab1',
          'charset':'utf8',
          'cursorclass':pymysql.cursors.Cursor,
          }

# 连接数据库
db = pymysql.connect(**config)

try:
    with db.cursor() as cursor:
        sql = 'select * from professor;'
        count = cursor.execute(sql) # 影响的行数
        print(count)
        result = cursor.fetchall()  # 取出所有行
 
        for i in result:            # 打印结果
            print(i)
        db.commit()         # 提交事务
except:
    db.rollback()           # 若出错了，则回滚
finally:
    print("good bye")
