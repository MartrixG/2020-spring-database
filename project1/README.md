#### 需求分析
##### 需求
   本组选择建立一个关于大学研究生人员管理的数据库。
##### 结构和约束
###### 结构
大学研究生应该包含以下几种人员或实体：教授、学生、部门、项目
###### 约束
1. 每个教授（professor）拥有一个唯一编号(SSN)，姓名(name)，年龄(age)，职称(rank)，研究方向(specipalty)，居住地址(location)
2. 每一个项目（projects）都有一个编号(number)，一个开始时间(start_time)，一个结束时间(end_time)，和预算（budget）
3. 学生（students）拥有一个唯一编号（SSN），姓名（name），年龄（age），年级（grade）
4. 每一个项目都被一位教授所管理（manage）（作为首席研究员）
5. 每一个项目会有一个至多个教授所参与研究（work_on）
6. 教授可以管理和参与研究多个项目
7. 每一个项目会有一个至多个学生参与研究（work_on）
8. 当学生参与某个项目的研究时，必须有个教授监督（supervise）学生的研究。每个学生可以参与研究多个项目，参加的每一个项目都需要一个教授进行监督。
9. 每一个部门（department）拥有一个部门编号（number），一个办公地点（office）
10. 每个部门需要一个教授进行管理（run）
11. 教授可以在多个部门进行工作，对于每一个工作的部门，会对教授工作的时间（time_percentage）进行记录
12. 学生需要主修（major）在一个部门
##### 性能要求
可以快速地对项目利用起止时间属性进行排序。
可以快速的对项目按照项目的负责人进行查询。
需要经常性对学生的所有信息进行查询，包括所属部门以及参加的项目。
需要快速地了解一个部门的人员结构。

#### 概念数据库设计
将上述的结构和约束表示为E-R图
![](1.png)
#### 逻辑数据库设计
##### 关系数据库模式
根据E-R图直接转换为关系数据库模式如下
professor(<u>SSN</u>, name, age, rank, specialty)

locations(<u>SSN</u>, location)  其中SSN参照professor.SSN

projects(<u>number</u>, start_time, end_time, budget, SSN)  其中SSN参照professor.SSN

students(<u>SSN</u>, name, age, grade, number)  其中number参照departments.number

departments(<u>number</u>, name, main_office, SSN)  其中SSN参照professor.SSN

N:N：  
works_on(<u>SSN,number</u>)其中SSN参照professor.SSN，number参照projects.number

works_in(<u>SSN,number</u>, time_percentage)  其中SSN参照professor.SSN，number参照departments.number

三元联系：
supervise(<u>PSSN, GSSN, number</u>)  其中PSSN参照professor.SSN， GSSN参照graduate_students.SSN， number参照projects.number

运用关系数据库规范化理论，对数据库模式进行规范化。
professor(<u>SSN</u>, name, age, rank, specialty)
函数依赖：考虑重名，name和age，rank，speclaity之间无依赖关系。满足BCNF

locations(<u>SSN</u>, <u>location</u>)  其中SSN参照professor.SSN
已经满足BCNF

projects(<u>number</u>, start_time, end_time, budget, SSN)  其中SSN参照professor.SSN
已经满足BCNF

students(<u>SSN</u>, name, age, grade, number)  其中number参照departments.number
函数依赖：考虑重名，name和age，grade，number之间无依赖关系。满足BCNF

departments(<u>number</u>, name, main_office, SSN)  其中SSN参照professor.SSN
函数依赖：部门名字不考虑重名，name->main_office，name->SSN，部分非主属性传递依赖于候选键
规范化(BCNF)：NN(<u>number</u>,name)    NM(<u>name</u>, main_office)   NS(<u>name</u>,SSN)

N:N：  
works_on(<u>SSN,number</u>)其中SSN参照professor.SSN，number参照projects.number
已经满足BCNF

works_in(<u>SSN,number</u>, time_percentage)  其中SSN参照professor.SSN，number参照departments.number
已经满足BCNF

三元联系：
supervise(<u>PSSN, GSSN, number</u>)  其中PSSN参照professor.SSN， GSSN参照graduate_students.SSN， number参照projects.number
已经满足BCNF

#### 物理数据库设计
##### 负载分析
人事处经常按项目的负责人和起止日期来查询项目信息。
人事处经常按项目的负责人查询其所负责的项目。
经常按查找教授负责的学生
经常按部门名字查找负责的教授
##### 调整数据库模式
1.	选择合适的数据类型，age可以使用TINYINT，SSN和number可以使用INT，start_date和ending_date可使用DATE类型，rank和specialty,name可使用字符串型
2.	考虑到效率，可以将NN(<u>number</u>,name)，NM(<u>name</u>, main_office)，NS(<u>name</u>,SSN)重新合并为departments(<u>number</u>, name, main_office, SSN)
3.	可以对students(<u>SSN</u>, name, age, grade, number)进行横向划分
##### 索引设计
create index idx1 on projects(SSN, starting_date, ending_date)
create index idx2 on projects(SSN)
create index idx3 on supervise(PSSN)
create index idx4 on department(name(X))   其中X根据实际设置，使选择度为1
#### 数据库建立
##### 应用需求分析
人事处经常按项目的负责人和起止日期来查询项目信息。
人事处经常需要查询负责人和其所负责的项目以及负责的部门。
各个部门经常需要统计本部门的学生所参加的项目、姓名、年级等信息。
各个部门经常需要查看本部门的人员结构以及本部门负责的项目信息。
##### 定义概念模式。
选用mySQL
create table professor(
PSSN CHAR(9) primary key,
Pname VARCHAR(10),
Page TINYINT,
Prank VARCHAR(20),
Pspecialty VARCHAR(20)
);

create table locations(
	PSSN CHAR(9) not null ,
	Location VARCHAR(30) not null,
	PRIMARY KEY (PSSN, Location)，
	FOREIGN KEY (PSSN) REFERENCES professor(PSSN)
);

create table projects(
	Pnumber INT primary key,
	Start_time DATE,
	End_time DATE,
Budget INT,
PSSN CHAR(9),
FOREIGN KEY (PSSN) REFERENCES professor(PSSN)
); 

create table students (
GSSN  CHAR(9) primary key,
Gname VARCHAR(10) not null,
Gage TINYINT,
Grade INT,
Dnumber INT,
FOREIGN KEY (Dnumber) REFERENCES departments(Dnumber)
); 

create table departments(
	Dnumber INT primary key,
	Dname VARCHAR(20),
	Main_office VARCHAR(20),
	PSSN CHAR(9),
	FOREIGN KEY (PSSN) REFERENCES professor(PSSN)
);

create table works_on(
	PSSN CHAR(9) primary key not null,
	Pnumber INT not null,
	PRIMARY KEY (PSSN, Pnumber),
	FOREIGN KEY (PSSN) REFERENCES professor(PSSN),
	FOREIGN KEY (Pnumber) REFERENCES projects(Pnumber));
);

create table works_in(
	PSSN CHAR(9) not null,
	Dnumber INT not null,
	Time_percentage INT,
	PRIMARY KEY (PSSN, Dnumber),
	FOREIGN KEY (PSSN) REFERENCES professor(PSSN),
	FOREIGN KEY (Dnumber) REFERENCES departments(Dnumber)
);

create table supervise(
	PSSN CHAR(9),
	GSSN CHAR(9),
	Pnumber INT,
	PRIMARY KEY (PSSN, GSSN,Pnumber),
	FOREIGN KEY (PSSN) REFERENCES professor(PSSN),
	FOREIGN KEY (GSSN) REFERENCES students(GSSN),
	FOREIGN KEY (Pnumber) REFERENCES projects(Pnumber)
);
##### 数据库视图
create view ProjectsStat as (select * from projects)
create view Leader as (select (name, SSN, departments.number, projects.number) from (professors join projetcs) join work_in)
create view StudStat as  (select * from (students join supervise))
create veiw DepartmentStat as (select * from (departments join works_on) join projects)

#### 总结和体会
1. 设计方案
在对数据库的设计中，首先对要求进行E-R图的抽象。接着进行关系数据模式进行设计，得到基础的关系表。然后利用函数依赖、数据库规范化理论对之前设计出的关系表进行规范化的优化。建立起数据库的内模式。接着根据数据库的工作负载，性能需求设计数据库索引。以及采用正确的符合性能需求的物理模式。最后根据应用需求利用已经设计的好的内模式设计出外模式，即需要的视图，方便用户的查询操作。
2. 心得体会
对于数据库的设计，按照一定的设计规范，按照步骤进行设计。就可以得到高可用、稳定性强条理清楚的数据库应用程序。