use webScrap; -- Database name

-- *********SETUP, DATA CLEANING

-- Basic queries for database connectivity and columns
describe skillsTable;
select * from skillsTable LIMIT 1;
select * from jobDescriptionTable LIMIT 10;
select * from equipmentTable LIMIT 1;
select * from jobLinkDataTable LIMIT 1;

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


-- Create a dictionary for job seniority
DROP TABLE IF EXISTS jobSeniorityTable;
CREATE TABLE jobSeniorityTable AS
select distinct jobSeniority,
CASE 
	WHEN jobSeniority = "Trainee" THEN "1. Trainee"
    WHEN jobSeniority = "Mid//Senior" THEN "6. Mid-Senior"
    WHEN jobSeniority = "Junior" THEN "3. Junior"
    WHEN jobSeniority = "Junior//Mid" THEN "4. Junior-Mid"
    WHEN jobSeniority = "Mid" THEN "5. Mid"
    WHEN jobSeniority = "Trainee//Junior" then "2. Trainee-Junior"
    WHEN jobSeniority = "Senior" then "6. Senior"
    WHEN jobSeniority = "Senior//Expert" then "7. Senior-Expert"
    WHEN jobSeniority = "Expert" then "8. Expert"
END AS jobSeniorityCategories
from jobDescriptionTable
ORDER BY jobSeniorityCategories;


-- Clean the job description table, extract some features
DROP TABLE IF EXISTS jobDescriptionTableFormat;
CREATE TABLE jobDescriptionTableFormat
SELECT 
-- perform stage 2 selection to calculate mean salary upon updated salary column
*, ROUND((salaryLow+salaryHighAdj)/2,0) as salaryMean
	FROM (
	SELECT 
	jobLink, jobId, collectDate, jobTitle, salaryLow,
    jobSeniorityCategories, -- from table containg dictionary of job levels
    -- clean up salary a little where some parts were cut into next column
	CASE WHEN salaryHigh < salaryLow then salaryLow ELSE salaryHigh END AS salaryHighAdj,
    -- fetch company name
	LTRIM(SUBSTRING(companyName,LENGTH(jobTitle)+2,LENGTH(companyName))) AS companyName,
    -- leave out only category name
    LTRIM(SUBSTRING(jobCategory, LENGTH('Kategoria: '),LENGTH(jobCategory))) AS jobCategory,
    -- extract compoyment type
    CASE 
	WHEN POSITION('UoP' IN salaryDetails) THEN 'Employment Contract'
	WHEN POSITION('na rękę' IN salaryDetails) THEN 'B2B'
	WHEN POSITION('B2B' IN salaryDetails) THEN 'B2B'
	ELSE 'OTHER'
	END AS employmentType
	FROM 
	jobDescriptionTable
    -- join job seniority dictionary
    LEFT JOIN jobSeniorityTable ON jobDescriptionTable.jobSeniority = jobSeniorityTable.jobSeniority
	WHERE
	salaryLow > 0
	LIMIT 100000
) as jobDescriptionTableFormat;

-- Update a key
ALTER TABLE jobDescriptionTableFormat
ADD PRIMARY KEY (jobId);

-- Export CSV
select * from jobDescriptionTableFormat;

-- Work in progress: analyse skill by salary and other features. 
-- Create view to matches both skills and job description, especially salary
DROP VIEW IF EXISTS VdescribedSkills;
CREATE VIEW VdescribedSkills AS
select
skillsTable.jobLink, skillsTable.skills, 
jobCategory, salaryLow, salaryHighAdj, salaryMean, jobSeniorityCategories
from 
skillsTable
left join jobDescriptionTableFormat  on skillsTable.jobLink = jobDescriptionTableFormat.jobLink
where salaryLow > 0;


-- Prepare most characteristic job Category per skill, to help skill categorization
DROP TABLE IF EXISTS SkillwithMainCategoryTable;
CREATE TABLE SkillwithMainCategoryTable AS
WITH added_row_number AS (
	SELECT *, 
    ROW_NUMBER() OVER(PARTITION BY skills ORDER BY N DESC) AS irow_number
	FROM (
		SELECT skills, jobCategory, count(skills) as N
		from jobXSkills
		group by jobCategory, SKILLS
		order by jobCategory, N DESC
	) as tempTABLE2
	group by skills, jobCategory
    ) 
SELECT *
FROM added_row_number
WHERE irow_number = 1;

-- Prepare dictionary table for excel processing and categorisation
SELECT TEMP.skills, TEMP.NumSkills,
SkillwithMainCategoryTable.jobCategory
FROM (    
	SELECT skills, count(skills) as NumSkills
	from VdescribedSkills
	group by skills
) as TEMP
left join SkillwithMainCategoryTable on TEMP.skills = SkillwithMainCategoryTable.skills ;

-- **** MANUAL CATEGORISATION IN EXCEL ***** --

-- IMPORT CATEGORIZED RESULT
-- 1/ create table template for the first run
DROP TABLE IF EXISTS skillsCategorised;
CREATE TABLE skillsCategorised (
	skills VARCHAR(500) PRIMARY KEY NOT NULL,
	skillsCount INT,
	prevailingJobCategory VARCHAR(500),
	manualAggregate VARCHAR(500),
	manualMetaAggregate VARCHAR(500),
	manualAggregateCount INT,
	skillsAgr VARCHAR(500),
	netAggregateCount INT
);

-- 2/ Load CSV DATA
SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE '/Users/grzegorzpopielnicki/Documents/GitHub/job-skill-recommender/Skills_Library.csv'
INTO TABLE skillsCategorised
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



-- 3/ match skills with aggregated skills
-- Replace ";" occurances to avoid join issues from csv
drop view if exists VdescribedSkillsAgr;
create view VdescribedSkillsAgr AS
select jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean,
jobSeniorityCategories, skillsCategorised.skillsAgr  
FROM (
select jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean,
jobSeniorityCategories, REPLACE(skills,';','//') as skills
from VdescribedSkills) AS TEMP
left join skillsCategorised on TEMP.skills = skillsCategorised.skills;

-- Export view to CSV;
select * from VdescribedSkillsAgr;

ALTER TABLE jobDescriptionTableFormat
ADD PRIMARY KEY (jobId);



-- 2. Which skills are most popular next to chosen skills?
-- Check skill's co-requirement. 
SET @skillSelection = 'SQL'; -- Set a variable for a skill to be investigated
-- Third table: compute Percentage and format
SELECT 
skills, skillCount, CONCAT(ROUND(skillCount / (MAX(skillCount) over())*100),"%") as skillCountPERC
FROM (
	-- Second table: left join all the skills (having only job offer with matching skills). Sum up
	SELECT 
	skills, count(skills) as skillCount
		FROM (
        -- First table: isolate the investiaged skill as selection
		SELECT 
        jobLink, skills as selectedSkill FROM VJobXSkills WHERE skills=@skillSelection
		) AS selection 
	LEFT JOIN VJobXSkills ON VJobXSkills.jobLink = selection.jobLink
	GROUP BY skills
	ORDER BY skillCount DESC
) as skillCountTable
GROUP BY skills;


-- What are salary ranges in categories for corresponding seniority levels?
SELECT 
jobCategory, jobSeniorityCategories, ROUND(AVG(salaryMean),0) AS salaryAVG, MIN(salaryLow) AS salaryMIN, MAX(salaryHighAdj) AS salaryMAX ,COUNT(*) AS NumberOfJobs
FROM
jobDescriptionTableFormat
GROUP BY jobCategory, jobSeniorityCategories
ORDER BY jobCategory, jobSeniorityCategories;

-- What are salaries per category?
SELECT 
jobCategory, ROUND(AVG(salaryMean),0) AS salaryAVG, MIN(salaryLow) AS salaryMIN, MAX(salaryHighAdj) AS salaryMAX ,COUNT(*) AS NumberOfJobs
FROM
jobDescriptionTableFormat
GROUP BY jobCategory
ORDER BY salaryAVG DESC;


-- *** FURTHER ANALYSIS IN TABLEAU **** --




