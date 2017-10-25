create table 2011TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2011TotalCount select count(*) from h1bpetitionsbyyear2011 where UPPER(job_title) like UPPER('%Data Engineer%');

create table 2012TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2012TotalCount select count(*) from h1bpetitionsbyyear2012 where UPPER(job_title) like UPPER('%Data Engineer%');

create table 2013TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2013TotalCount select count(*) from h1bpetitionsbyyear2013 where UPPER(job_title) like UPPER('%Data Engineer%'); 

create table 2014TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2014TotalCount select count(*) from h1bpetitionsbyyear2014 where UPPER(job_title) like UPPER('%Data Engineer%');

create table 2015TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2015TotalCount select count(*) from h1bpetitionsbyyear2015 where UPPER(job_title) like UPPER('%Data Engineer%');

create table 2016TotalCount (count Double) row format delimited fields terminated by '\t';
insert overwrite table 2016TotalCount select count(*) from h1bpetitionsbyyear2016 where UPPER(job_title) like UPPER('%Data Engineer%');

select ROUND(((((b.count-a.count)/a.count)+((c.count-b.count)/b.count)+((d.count-c.count)/c.count)+((e.count-d.count)/d.count)+((f.count-e.count)/e.count))/5)*100,2) from 2011TotalCount a, 2012TotalCount b,2013TotalCount c, 2014TotalCount d, 2015TotalCount e, 2016TotalCount f;
