
select department,job, 
Q1=sum(case when Quartile=1 then 1 else 0 end ),
Q2=sum(case when Quartile=2 then 1 else 0 end ),
Q3=sum(case when Quartile=3 then 1 else 0 end ),
Q4=sum(case when Quartile=4 then 1 else 0 end )
from(
	select b.department,c.job, NTILE(4) OVER (PARTITION BY month(convert(datetime,[datetime])) ORDER BY department) AS Quartile 
	from hired_employees a 
	inner join departments b on a.department_id=b.id
	inner join jobs c on a.job_id=c.id
	where year(convert(datetime,datetime))='2021'
	--order by b.department,c.job
) a 
group by department,job

drop table hired_employees

create table hired_employees(
	ID_AI int NOT NULL IDENTITY(1, 1),
	id int,
	Name varchar(200),
	datetime varchar(200),
	department_id int,
	job_id int
)

drop table jobs

create table jobs(
	ID_AI int NOT NULL IDENTITY(1, 1),
	id int,
    job varchar (200)
)

drop table departments

create table departments(
	ID_AI int NOT NULL IDENTITY(1, 1),
	id int,
    department varchar (200)
)