import mySqlHandler


class Filter():
    
    def __init__(self):
        self.sql = mySqlHandler.MySqlHandler()

    # 根据项目起止日期查询项目
    def getProjectbyTime(self, Start_time, End_time):
        return self.sql.getProjectbyTime(Start_time, End_time)

    # 根据项目负责人查询项目
    def getProjectbyProfessor(self, professor_PSSN):
        return self.sql.getProjectbyProfessor(professor_PSSN)

    # 根据项目起止日期和项目负责人查询项目
    def getProjectbyProfessor_Time(self, Start_time, End_time, professor_PSSN):
        return self.sql.getProjectbyProfessor_Time(Start_time, End_time, professor_PSSN)

    # 根据教授查找负责的学生
    def getStudentsbyProfessor(self, professor_PSSN):
        return self.sql.getStudentsbyProfessor(professor_PSSN)

    #根据部门名字查找负责的教授以及时间百分比
    def getProfessorbyDepartment(self, department):
        return self.sql.getProfessorbyDepartment(department)
    
    #根据部门统计学生信息（所参加的项目、姓名、年级）
    def getStudentsInfobyDepartment(self, department):
        return self.sql.getStudentsInfobyDepartment(department)
    




