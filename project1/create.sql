--create database

create table professor(
    PSSN CHAR(9) PRIMARY KEY,
    Pname VARCHAR(10),
    Page TINYINT,
    Prank VARCHAR(20),
    Pspecialty VARCHAR(20)
);

create table locations(
	PSSN CHAR(9) not null ,
	Location VARCHAR(30) not null,
	primary key (PSSN, Location)，
	foreign key (PSSN) references professor(PSSN)
);

create table projects(
	Pnumber INT primary key,
	Start_time DATE,
	End_time DATE,
    Budget INT,
    PSSN CHAR(9),
    foreign key (PSSN) REFERENCES professor(PSSN)
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
	PSSN CHAR(9) not null,
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
