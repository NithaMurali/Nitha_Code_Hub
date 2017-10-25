yearlyPetitions = Load '/user/hive/warehouse/h1b_final' USING PigStorage('\t') AS(s_no,case_status, soc_name,employer_name,job_title:chararray,full_time_position,prevailing_wage,year:chararray,worksite);

yearlyJobs = foreach yearlyPetitions GENERATE LOWER($4) as job_title, $7 as year;

DataEnggJobs = filter yearlyJobs BY (job_title matches '.*data engineer.*');

yearlyGroupedDataEng = Group DataEnggJobs by year;

dataEnggYearlyCount = foreach yearlyGroupedDataEng Generate $0 as year, COUNT($1) as count;

dump dataEnggYearlyCount;

