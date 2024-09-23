create database metric_spike;

use metric_spike;

create table users (
user_id integer,
created_at varchar(100),
company_id integer,
language varchar(30),
activated datetime,
state varchar(20));

ALTER TABLE users
MODIFY COLUMN activated VARCHAR(100);

show variables LIKE 'secure_file_priv';
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from users;
use metric_spike;

create table events (
user_id integer,
occured_at varchar(100),
event_type varchar(30),
event_name varchar(30),
location varchar(10),
device varchar(20),
user_type integer);

ALTER TABLE events
MODIFY COLUMN device varchar(100);

ALTER TABLE events
MODIFY COLUMN location varchar(100);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


create table email_events (
user_id integer,
occured_at varchar(100),
action varchar(100),
user_type integer);

ALTER TABLE events
RENAME COLUMN occured_at TO occurred_at;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
select * from email_events;

select * from users;

-- Step 1: Add a new DATETIME column
ALTER TABLE users
ADD COLUMN created_at_new DATETIME;

SET SQL_SAFE_UPDATES = 0;
-- Step 2: Update the new column with converted data
UPDATE users
SET created_at_new = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

-- Step 3: Drop the old column
ALTER TABLE users
DROP COLUMN created_at;

-- Step 4: Rename the new column
ALTER TABLE users
CHANGE COLUMN created_at_new created_at DATETIME;
select * from users;

ALTER TABLE users
ADD COLUMN activated_new DATETIME;

SET SQL_SAFE_UPDATES = 0;
-- Step 2: Update the new column with converted data
UPDATE users
SET activated_new = STR_TO_DATE(activated, '%d-%m-%Y %H:%i');

-- Step 3: Drop the old column
ALTER TABLE users
DROP COLUMN activated;
-- Step 4: Rename the new column
ALTER TABLE users
CHANGE COLUMN activated_new activated DATETIME;

Alter table events ADD COLUMN occurred_at_new DATETIME;
UPDATE events set occurred_at_new=str_to_date(occurred_at,'%d-%m-%Y %H:%i');
Alter Table events drop column occurred_at;
alter table events rename column occurred_at_new to occured_at;

select * from email_events;

alter table email_events add column occurred_at_new datetime;
UPDATE email_events set occurred_at_new=str_to_date(occured_at,'%d-%m-%Y %H:%i');
alter table email_events drop column occured_at;
alter table email_events rename column occurred_at_new to occurred_at;

select * from users,events,email_events;

#Objective: Measure the activeness of users on a weekly basis.
#Your Task: Write an SQL query to calculate the weekly user engagement.

-- To check current timeout settings
SHOW VARIABLES LIKE '%timeout%';

-- To set new timeout values (adjust as needed)
SET GLOBAL connect_timeout = 1000;        -- 5 minutes
SET GLOBAL wait_timeout = 28800;         -- 8 hours
SET GLOBAL interactive_timeout = 28800;  -- 8 hours

-- For the current session only
SET SESSION wait_timeout = 28800;
SET SESSION interactive_timeout = 28800;

SET GLOBAL max_execution_time = 60000; -- 60 seconds
SET GLOBAL innodb_buffer_pool_size = 1073741824; -- 1GB
SET GLOBAL tmp_table_size = 134217728; -- 128MB
SET GLOBAL max_heap_table_size = 134217728; -- 128MB

select * from users;
select * from events;
describe events;
use metric_spike;
SELECT
     DATE(e.occured_at - INTERVAL DAYOFWEEK(e.occured_at) - 1 DAY) AS week,
    COUNT(DISTINCT e.user_id) AS active_users,
    COUNT(DISTINCT u.user_id) AS total_users,
    ROUND(CAST(COUNT(DISTINCT e.user_id) AS FLOAT) / CAST(COUNT(DISTINCT u.user_id) AS FLOAT) * 100, 2) AS engagement_rate
FROM events e
JOIN users u ON u.activated <= e.occured_at
GROUP BY DATE(e.occured_at - INTERVAL DAYOFWEEK(e.occured_at) - 1 DAY)
ORDER BY week;
use metric_spike;
CREATE INDEX idx_events_occurred_at_user_id ON events(occured_at, user_id);
CREATE INDEX idx_users_activated_at ON users(activated);
SELECT
    DATE(e.occured_at - INTERVAL (DAYOFWEEK(e.occured_at) - 1) DAY) AS week,
    COUNT(DISTINCT e.user_id) AS active_users,
    (SELECT COUNT(*) FROM users WHERE activated <= MAX(e.occured_at)) AS total_users,
    ROUND(COUNT(DISTINCT e.user_id) / 
        (SELECT COUNT(*) FROM users WHERE activated <= MAX(e.occured_at)) * 100, 2) AS engagement_rate
FROM events e
WHERE e.occured_at >= (SELECT MIN(activated) FROM users)
GROUP BY DATE(e.occured_at - INTERVAL (DAYOFWEEK(e.occured_at) - 1) DAY)
ORDER BY week;

select * from users;
use metric_spike;

SELECT 
    DATE_FORMAT(occured_at, '%Y-%m-01') AS month,
    COUNT(DISTINCT user_id) AS new_users,
    SUM(COUNT(DISTINCT user_id)) OVER (ORDER BY DATE_FORMAT(occured_at, '%Y-%m-01')) AS cumulative_users
FROM (SELECT user_id, MIN(occured_at) AS occured_at FROM events GROUP BY user_id) AS first_events
GROUP BY DATE_FORMAT(occured_at, '%Y-%m-01')
ORDER BY month;
    
SELECT * from users,events;
select u.user_id, u.created_at, COUNT(e.occured_at) AS total_retention from users u
join events e on e.user_id=u.user_id
group by COUNT(e.occured_at)
order by u.user_id;

SELECT DATE(u.created_at - INTERVAL DAYOFWEEK(u.created_at) - 1 DAY) AS signup_week,
COUNT(DISTINCT u.user_id) AS total_users,
COUNT(DISTINCT CASE WHEN e.occured_at IS NOT NULL THEN u.user_id END) AS retained_users
FROM users u LEFT JOIN events e ON u.user_id = e.user_id
GROUP BY DATE(u.created_at - INTERVAL DAYOFWEEK(u.created_at) - 1 DAY)
ORDER BY signup_week;

select * from users,events,email_events;
 SELECT DATE(occured_at - INTERVAL DAYOFWEEK(occured_at) - 1 DAY) AS Per_week,
 device,
 count(DISTINCT user_id) AS active_users, count(*) as total_events FROM events
 group by DATE(occured_at - INTERVAL DAYOFWEEK(occured_at) - 1 DAY),device
 order by per_week;
 
 SELECT * from email_events;
 SELECT 
 DATE_FORMAT(occurred_at, '%Y-%m-01') AS per_month,
 count(DISTINCT user_id) AS no_of_users, count(*) as total_engagement FROM email_events
 group by 
DATE_FORMAT(occurred_at, '%Y-%m-01')
 order by 
 per_month;