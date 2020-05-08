import tkinter
import filter


class ManagerGUI():
    
    def __call__(self):
        self.filter = filter.Filter()
        self.tk = tkinter.Tk()
        self.tk.title("查询")
        self.tk.geometry('800x300')
        self.tk.resizable(0,0)
        
        self.label1 = tkinter.Label(self.tk, text="请输入查询信息", compound='left')
        self.label1.grid(row=0, column=0, sticky=tkinter.W, pady=10)
        
        self.label4 = tkinter.Label(self.tk, text="项目开始日期", compound='left')
        self.label4.grid(row=3, column=0, sticky=tkinter.W, pady=10)
        self.entry3 = tkinter.Entry(self.tk, width=26)
        self.entry3.grid(row=3, column=1, sticky=tkinter.W, pady=10)
        self.label5 = tkinter.Label(self.tk, text="项目结束日期", compound='left')
        self.label5.grid(row=4, column=0, sticky=tkinter.W, pady=10)
        self.entry4 = tkinter.Entry(self.tk, width=26)
        self.entry4.grid(row=4, column=1, sticky=tkinter.W, pady=10)
        self.label6 = tkinter.Label(self.tk, text="项目负责人", compound='left')
        self.label6.grid(row=5, column=0, sticky=tkinter.W, pady=10)
        self.entry5 = tkinter.Entry(self.tk, width=26)
        self.entry5.grid(row=5, column=1, sticky=tkinter.W, pady=10)
        self.label7 = tkinter.Label(self.tk, text="教授", compound='left')
        self.label7.grid(row=6, column=0, sticky=tkinter.W, pady=10)
        self.entry6 = tkinter.Entry(self.tk, width=26)
        self.entry6.grid(row=6, column=1, sticky=tkinter.W, pady=10)
        self.label8 = tkinter.Label(self.tk, text="部门", compound='left')
        self.label8.grid(row=7, column=0, sticky=tkinter.W, pady=10)
        self.entry7 = tkinter.Entry(self.tk, width=26)
        self.entry7.grid(row=7, column=1, sticky=tkinter.W, pady=10)

        
        self.button5 = tkinter.Button(self.tk, text="根据项目起止日期查询项目", activeforeground="red", command=self.getProjectbyTime)
        self.button5.grid(row = 3, column=3, sticky=tkinter.W)
        self.button6 = tkinter.Button(self.tk, text="根据项目负责人查询项目", activeforeground="red", command=self.getProjectbyProfessor)
        self.button6.grid(row = 4, column=3, sticky=tkinter.W)
        self.button5 = tkinter.Button(self.tk, text="根据项目起止日期和项目负责人查询项目", activeforeground="red", command=self.getProjectbyProfessor_Time)
        self.button5.grid(row = 5, column=3, sticky=tkinter.W)
        self.button6 = tkinter.Button(self.tk, text="根据教授查找负责的学生", activeforeground="red", command=self.getStudentsbyProfessor)
        self.button6.grid(row = 6, column=3, sticky=tkinter.W)
        self.button6 = tkinter.Button(self.tk, text="根据部门名字查找负责的教授以及时间百分比", activeforeground="red", command=self.getProfessorbyDepartment)
        self.button6.grid(row = 7, column=3, sticky=tkinter.W)
        self.button6 = tkinter.Button(self.tk, text="根据部门统计学生信息", activeforeground="red", command=self.getStudentsInfobyDepartment)
        self.button6.grid(row = 8, column=3, sticky=tkinter.W)

        
        self.tk.mainloop()

    
    def getProjectbyTime(self):
        start_time = self.entry3.get()
        end_time = self.entry4.get()
        ret = self.filter.getProjectbyTime(start_time, end_time)
        ret = (str)(ret)
    
        print('project_number: ' + ret[2])
    
    def getProjectbyProfessor(self):
        PSSN = self.entry5.get()
        ret = self.filter.getProjectbyProfessor(PSSN)
        ret = (str)(ret)

        print('project_number: ' + ret[2])

    def getProjectbyProfessor_Time(self):
        start_time = self.entry3.get()
        end_time = self.entry4.get()
        PSSN = self.entry5.get()
        ret = self.filter.getProjectbyProfessor_Time(start_time, end_time, PSSN)
        ret = (str)(ret)

        print('project_number: ' + ret[2])

    def getStudentsbyProfessor(self):
        PSSN = self.entry6.get()
        ret = self.filter.getStudentsbyProfessor(PSSN)
        ret = (str)(ret)
        
        print('student_GSSN: ' + ret[3])

    def getProfessorbyDepartment(self):
        department = self.entry7.get()
        ret = self.filter.getProfessorbyDepartment(department)
        ret = (str)(ret)
        
        print('professor_PSSN: ' + ret[3])
        print('time_percentge: ' + ret[7] + ret[8])

    def getStudentsInfobyDepartment(self):
        department = self.entry7.get()
        ret = self.filter.getStudentsInfobyDepartment(department)
        ret = (str)(ret)
        
        print('student_name: ' + ret[3:10])
        print('grade: ' + ret[14] + ret[15])

    def tipGUI(self, title, text, root):
        tkError = tkinter.Toplevel(root)
        tkError.title(title)
        tkError.geometry('300x80')
        tkError.resizable(0,0)
        lable = tkinter.Label(tkError, text = text, font=8, fg="red")
        lable.grid(row = 0, column=0, pady=23, padx = 70)
        tkError.mainloop()

GUI = ManagerGUI()
GUI()

