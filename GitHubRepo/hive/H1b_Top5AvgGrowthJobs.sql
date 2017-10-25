create table 2011table (job_title string,year string)
row format delimited                                                                                  
fields terminated by '\t';
insert overwrite table 2011table select  job_title,year from h1bPetitionsByYear2011;

create table 2011PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2011PetitionCountForJob select job_title,count(*) as count from 2011table group by job_title;

create table 2012table (job_title string,year string)
row format delimited                                                                                  
fields terminated by '\t';
insert overwrite table 2012table select  job_title,year from h1bPetitionsByYear2012;
create table 2012PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2012PetitionCountForJob select job_title,count(*) as count from 2012table group by job_title;

create table 2013table (job_title string,year string) row format delimited fields terminated by '\t';
insert overwrite table 2013table select  job_title,year from h1bPetitionsByYear2013;
create table 2013PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2013PetitionCountForJob select job_title,count(*) as count from 2013table group by job_title;

create table 2014table (job_title string,year string) row format delimited fields terminated by '\t';
insert overwrite table 2014table select  job_title,year from h1bPetitionsByYear2014;
create table 2014PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2014PetitionCountForJob select job_title,count(*) from 2014table group by job_title;

create table 2015table (job_title string,year string) row format delimited fields terminated by '\t';
insert overwrite table 2015table select  job_title,year from h1bPetitionsByYear2015;
create table 2015PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2015PetitionCountForJob select job_title,count(*) as count from 2015table group by job_title;

create table 2016table (job_title string,year string) row format delimited fields terminated by '\t';
insert overwrite table 2016table select  job_title,year from h1bPetitionsByYear2016;
create table 2016PetitionCountForJob (job_title string,count Double) row format delimited fields terminated by '\t';
insert overwrite table 2016PetitionCountForJob select job_title,count(*) as count from 2016table group by job_title;

create table joinTable (job_title string,year2011Count Double,year2012Count Double,year2013Count Double,year2014Count Double,year2015Count Double,year2016Count Double) row format delimited fields terminated by '\t';
insert overwrite table joinTable select a.job_title,a.count, b.count, c.count, d.count , e.count, f.count from 2011PetitionCountForJob a join 2012PetitionCountForJob b on (a.job_title=b.job_title) join 2013PetitionCountForJob c on (b.job_title=c.job_title) join 2014PetitionCountForJob d on c.job_title=d.job_title join 2015PetitionCountForJob e on e.job_title=d.job_title join 2016PetitionCountForJob f on f.job_title=e.job_title;

create table growthTable (job_title string,growth2011 Double,growth2012 Double,growth2013 Double,growth2014 Double,growth2015 Double) row format delimited fields terminated by '\t';
insert overwrite table growthTable select job_title,(year2012Count - year2011Count)/year2011Count,(year2013Count - year2012Count)/year2012Count,(year2014Count - year2013Count)/year2013Count,(year2015Count - year2016Count)/year2016Count,(year2016Count - year2015Count)/year2015Count from joinTable;

select job_title,ROUND(((growth2011+growth2012+growth2013+growth2014+growth2015)/5),2) as avgGrowth from growthTable order by avgGrowth desc limit 5; 
