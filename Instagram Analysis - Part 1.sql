create database jobs;
use jobs;
create table job_data(
ds Date, job_id INT PRIMARY KEY, actor_id INT, event varchar(10),language VARCHAR(20), time_spent INT, org VARCHAR(20));
use jobs;
INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org)
VALUES 
    ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
    ('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
    ('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
    ('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
    ('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
    ('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
    ('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
    ('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');
    
USE jobs;
ALTER TABLE job_data
DROP PRIMARY KEY;

select * from job_data;

select ds, count(*) as total_jobs, sum(time_spent) as total_time,
 round(count(*)/(sum(time_spent)/3600),2) as jobs_reviewed_per_hour from job_data
where ds between '2020-11-01' and '2020-11-30' group by ds order by ds;

select * from job_data;

USE jobs;
SELECT current_day.ds,COUNT(*) / SUM(time_spent) AS daily_throughput,
    (SELECT COUNT(*) / SUM(time_spent) FROM job_data past_week
    WHERE past_week.ds BETWEEN
    current_day.ds - INTERVAL 6 DAY AND current_day.ds)AS rolling_7day_avg_throughput
    FROM job_data current_day GROUP BY current_day.ds ORDER BY current_day.ds;
    
    
USE jobs;    
SELECT language, count(*) as language_count,count(*)*100 / (select count(*) FROM job_data) as percentage_share
FROM job_data GROUP BY language ORDER BY percentage_share DESC;

USE jobs;
SELECT 
    ds, job_id, actor_id, event, language, time_spent, org, 
    COUNT(*) as duplicate_count
FROM 
    job_data
GROUP BY 
    ds, job_id, actor_id, event, language, time_spent, org
HAVING 
    COUNT(*) > 1
ORDER BY 
    duplicate_count DESC;
    
    
SELECT 
    job_id, 
    COUNT(*) as duplicate_count
FROM 
    job_data
GROUP BY 
    job_id
HAVING 
    COUNT(*) > 1
ORDER BY 
    duplicate_count DESC;