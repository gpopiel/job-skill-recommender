use webScrap; -- Database name

-- *********SETUP, DATA CLEANING

-- Basic queries for database connectivity and columns
describe skillsTable;
select * from skillsTable LIMIT 1;
select * from jobDescriptionTable LIMIT 1;
select * from equipmentTable LIMIT 1;

-- How many skill records
select count(*) from skillsTable;

-- PART 1: DUPLICATES REMOVAL
-- STEP 1: Prepare a view
-- Count distinct skills (to remove duplicates)
create view VJobXSkills AS
select 
skillId, jobLink, skills, 
concat(jobId, skills) as jobXSkills
from
skillsTable;

-- STEP2: Isolate out only the duplicates to check
SELECT
jobXSkills, count(jobXSkills)
FROM
VjobXSkills
GROUP BY
jobXSkills
HAVING
COUNT(jobXSkills) > 1;

-- STEP3: REMOVE DUPLICATES using inner join (performance check needed, causing timeouts). 
SET SQL_SAFE_UPDATES = 0; -- Disable safe updates
	DELETE t1 FROM VjobXSkills t1
	INNER JOIN VJobXSkills t2
	WHERE
	t1.skillId < t2.skillId AND
	t1.jobXSkills = t2.jobXSkills;
SET SQL_SAFE_UPDATES = 1; -- Re-enable safe updates


-- *********SETUP, DATA CLEANING
-- 1. Which skills are most popular altogether?
select 
skills, count(skills) as numberOfOffers 
from 
VJobXSkills
group by skills
order by numberOfOffers DESC;


-- 2. Which skills are most popular next to chosen skills?
-- Check skill's co-requirement. 
SET @skillSelection = 'SQL'; -- Set a variable for a skill to be investigated
-- Third table: compute Percentage and format
SELECT skills, skillCount, CONCAT(ROUND(skillCount / (MAX(skillCount) over())*100),"%") as skillCountPERC
FROM (
	-- Second table: left join all the skills (having only job offer with matching skills). Sum up
	SELECT 
	skills, count(skills) as skillCount
		FROM (
        -- First table: isolate the investiaged skill as selection
		SELECT jobLink, skills as selectedSkill FROM VJobXSkills WHERE skills=@skillSelection
		) AS selection 
	LEFT JOIN VJobXSkills ON VJobXSkills.jobLink = selection.jobLink
	GROUP BY skills
	ORDER BY skillCount DESC
) as skillCountTable
group by skills;

-- Work in progress: analyse skill by salary and other features. 

select
skillsTable.jobLink, skillsTable.skills, 
jobDescriptionTable.salaryLow, jobDescriptionTable.salaryHigh
from 
skillsTable
left join jobDescriptionTable
on skillsTable.jobLink = jobDescriptionTable.jobLink
where salaryLow > 0;
