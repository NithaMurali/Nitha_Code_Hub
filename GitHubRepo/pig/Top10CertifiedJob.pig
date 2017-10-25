--Find the most popular top 10 job positions for H1B visa applications for each year?
--pig -f Top10CertifiedJob.pig -param input=/user/hive/warehouse/h1bpetitionsbyyear2011

--mr-jobhistory-daemon.sh --config /usr/local/hadoop/etc/hadoop start historyserver

yearPetitions = Load '$input' USING PigStorage('\t') AS
(case_status:chararray,soc_name,employer_name,job_title:chararray,full_time_position,prevailing_wage,worksite);

yearlyCertifiedJobs = FILTER yearPetitions BY (case_status=='CERTIFIED' OR case_status=='CERTIFIED_WITHDRAWN');

yearlyJobs = foreach yearlyCertifiedJobs GENERATE LOWER($3) as job_title;

yearGroupedJobs = Group yearlyJobs BY job_title;

yearJobCount = foreach yearGroupedJobs Generate $0, COUNT(yearlyJobs.job_title) as count;

yearTopJob = order yearJobCount by count desc;

top10Job = LIMIT yearTopJob 10;

dump;



