create table AlljobTable (job_title string,job_count double)row format delimited fields terminated by '\t';

insert overwrite table AlljobTable select job_title,count(*) from h1b_final group by job_title;

create table AllCertJobTable (job_title string,job_count double)row format delimited fields terminated by '\t';

insert overwrite table AllCertJobTable select job_title,count(*) from h1b_final where case_status=='CERTIFIED' OR case_status=='CERTIFIED_WITHDRAWN'  group by job_title;

create table JoinedJobTable (job_title string,AllJob_count double,CertJob_count double )row format delimited fields terminated by '\t';

insert overwrite table JoinedJobTable select a.job_title,a.job_count, b.job_count from AllJobTable a join AllCertJobTable b on (a.job_title=b.job_title);

create table PercentJobTable (job_title string,petition_count double,petition_percentage double)row format delimited fields terminated by '\t';
insert overwrite table PercentJobTable select job_title,CertJob_count,ROUND(((CertJob_count/AllJob_count)*100),2) from JoinedJobTable;

create table Success70JobTable (job_title string,petition_count double,petition_percentage double)row format delimited fields terminated by '\t';

insert overwrite table Success70JobTable select job_title,petition_count,petition_percentage from PercentJobTable where petition_percentage >= 70.00;

--select * from Success70JobTable order by petition_percentage desc;

select * from Success70JobTable where petition_count >= 1000 order by petition_count desc;

