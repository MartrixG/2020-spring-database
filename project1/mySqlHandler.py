import pymysql
import time 
import traceback

class MySqlHandler:
    def __init__(self):
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
        self.db = pymysql.connect(**config)
        self.cursor = self.db.cursor()

    #根据项目起止日期查询项目
    def getProjectbyTime(self, Start_time, End_time):
        sql = "select Pnumber from projects where Start_time = '{}' and End_time = '{}';".format(Start_time, End_time)
    
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    #根据项目负责人查询项目
    def getProjectbyProfessor(self, professor_PSSN):
        sql = "select Pnumber from projects where PSSN = '{}';".format(professor_PSSN)
    
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result  
    
    #根据项目起止日期和项目负责人查询项目
    def getProjectbyProfessor_Time(self, Start_time, End_time, professor_PSSN):
        sql = "select Pnumber from projects where Start_time = '{}' and End_time = '{}' and PSSN = '{}';".format(Start_time, End_time, professor_PSSN)
    
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result  

    #根据部门统计学生信息（所参加的项目、姓名、年级）
    def getStudentsInfobyDepartment(self, department):
        sql_1 = "select GSSN from students where Dnumber = '{}';".format(department)
        self.cursor.execute(sql_1)
        result_1 = self.cursor.fetchall()
        
        result_1 = (str)(result_1)
        result_1 = result_1[3] + result_1[4]

        sql_2 = "select Gname,Grade from students where GSSN = '{}';".format(result_1)
        self.cursor.execute(sql_2)
        result_2 = self.cursor.fetchall()
        result_2 = (str)(result_2)
        
        return result_2

    #根据教授查找负责的学生
    def getStudentsbyProfessor(self, professor_PSSN):
        sql = "select GSSN from supervise where PSSN = '{}';".format(professor_PSSN)
    
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    #根据部门名字查找负责的教授以及时间百分比
    def getProfessorbyDepartment(self, department):
        sql = "select PSSN,Time_percentage from works_in where PSSN = '{}';".format(department)
    
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result