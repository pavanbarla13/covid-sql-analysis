Create database covid_project;

USE covid_project;

CREATE TABLE covid_data (
  location VARCHAR(100),
  date DATE,
  total_cases BIGINT,
  new_cases BIGINT,
  total_deaths BIGINT,
  new_deaths BIGINT,
  population BIGINT
  );
  
SELECT count(*) FROM covid_data;

SELECT * FROM covid_data;

/* Total cases vs total deaths (death percentage) */

select location,
date,total_cases,
total_deaths,(total_deaths/total_cases) * 100 AS Death_percentage 
from covid_data
where total_cases > 0
order by location, date;

/* Countries with highest infection rate */
select location, 
max(total_cases) AS Highest_cases,
max(total_cases / population) * 100 AS infection_percentage
from covid_data
group by location 
order by infection_percentage desc;

/* Death percentage per country*/
select location,
max(total_cases) AS Highest_cases,
max(total_deaths) AS Highest_deaths,
(max(total_deaths)/max(total_cases)) AS Death_percentage
from covid_data
where total_cases > 0
group by location
order by Death_percentage desc;

/* Rolling total cases (windows function)*/
select 
location,
date,
new_cases,
SUM(new_cases) over(partition by location order by date) AS rolling_total_cases
from covid_data;

/* Compare yesterday vs today */
select 
location,
date,
lag(new_cases) over (partition by location order by date) AS Previous_day_cases
from covid_data;

/* Calculate daily growth % properly */
WITH growth_data as (
select 
location,
date,
new_cases,
lag(new_cases) over ( partition by location order by date) AS previous_day_cases, /*yesteday cases*/
((new_cases - lag(new_cases) over (partition by location order by date ))
/ NULLIF(lag(new_cases) over ( partition by location order by date),0)) * 100 AS daily_growth_percentage from covid_data)
select 
location,
avg(daily_growth_percentage) AS avg_growth_percentage
from growth_data
where daily_growth_percentage is not null
group by location
order by avg_growth_percentage desc;

/* Daily growth % */
select 
location,
date,
new_cases,
lag(new_cases) over (partition by location order by date) AS previous_day_cases,
((new_cases- lag(new_cases) over (partition by location order by date))/
NULLIF(lag(new_cases) over ( partition by location order by date),0)) * 100 AS daily_growth_percentage from covid_data;

/* Rank countries by infection rate*/

With infection_rate as(
select 
location,
max(total_cases/population) * 100 AS infection_percentage 
from covid_data
group by location
)
select location,
infection_percentage,
rank() over (order by infection_percentage desc) as rank_position
from infection_rate;

/* Ttp 10 worst countries*/
With infection_rate as(
select 
location,
max(total_cases/population) * 100 AS infection_percentage 
from covid_data
group by location
)
select location,
infection_percentage
from infection_rate
order by infection_percentage desc 
limit 10;
 
/* Highest spike growth by country */
with growth_data as(
select 
location,
((new_cases- lag(new_cases) over (partition by location order by date))/
NULLIF(lag(new_cases) over ( partition by location order by date),0)) * 100 AS growth_percentage from covid_data
)
select location,
max(growth_percentage) as highest_spike
from growth_data
where growth_percentage is not null
group by location
order by highest_spike desc;

/* Monthly global trend*/
select
year(date) as year,
month(date) as month,
sum(new_cases) as total_cases
from covid_data 
group by year(date), month(date)
order by year, month;

/* Find peak global day */
select 
date,
sum(new_cases) as global_cases
from covid_data
group by date
order by global_cases desc
limit 1;












