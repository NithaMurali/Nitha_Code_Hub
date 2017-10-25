create table AllEmpTable (employer_name string,emp_count double)row format delimited fields terminated by '\t';
insert overwrite table AllEmpTable select employer_name,count(*) from h1b_final group by employer_name;

create table AllCertEmpTable (employer_name string,emp_count double)row format delimited fields terminated by '\t';
insert overwrite table AllCertEmpTable select employer_name,count(*) from h1b_final where case_status=='CERTIFIED' OR case_status=='CERTIFIED_WITHDRAWN'  group by employer_name;

create table JoinedEmpTable (employer_name string,Allemp_count double,Certemp_count double )row format delimited fields terminated by '\t';
insert overwrite table JoinedEmpTable select a.employer_name,a.emp_count, b.emp_count from AllEmpTable a join AllCertEmpTable b on (a.employer_name=b.employer_name);

create table PercentTable (employer_name string,petition_count double,petition_percentage double)row format delimited fields terminated by '\t';
insert overwrite table PercentTable select employer_name,Certemp_count,ROUND(((Certemp_count/Allemp_count)*100),2) from JoinedEmpTable;

create table Success70EmpTable (employer_name string,petition_count double,petition_percentage double)row format delimited fields terminated by '\t';
insert overwrite table Success70EmpTable select employer_name,petition_count,petition_percentage from PercentTable where petition_percentage >= 70.00;

--select * from Success70EmpTable order by petition_percentage desc;
select * from Success70EmpTable where petition_count >= 1000 order by petition_count desc;
