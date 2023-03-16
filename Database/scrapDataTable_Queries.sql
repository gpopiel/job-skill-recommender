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
DROP VIEW IF EXISTS VJobXSkills;
CREATE VIEW VJobXSkills AS
    SELECT 
        skillId,
        jobLink,
        skills,
        CONCAT(jobId, skills) AS jobXSkills
    FROM
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
CREATE TABLE jobSeniorityTable AS SELECT DISTINCT jobSeniority,
    CASE
        WHEN jobSeniority = 'Trainee' THEN '1. Trainee'
        WHEN jobSeniority = 'Mid//Senior' THEN '6. Mid-Senior'
        WHEN jobSeniority = 'Junior' THEN '3. Junior'
        WHEN jobSeniority = 'Junior//Mid' THEN '4. Junior-Mid'
        WHEN jobSeniority = 'Mid' THEN '5. Mid'
        WHEN jobSeniority = 'Trainee//Junior' THEN '2. Trainee-Junior'
        WHEN jobSeniority = 'Senior' THEN '6. Senior'
        WHEN jobSeniority = 'Senior//Expert' THEN '7. Senior-Expert'
        WHEN jobSeniority = 'Expert' THEN '8. Expert'
    END AS jobSeniorityCategories FROM
    jobDescriptionTable
ORDER BY jobSeniorityCategories;


-- Clean the job description table, extract some features
DROP TABLE IF EXISTS jobDescriptionTableFormat;
CREATE TABLE jobDescriptionTableFormat SELECT *, ROUND((salaryLow + salaryHighAdj) / 2, 0) AS salaryMean FROM
    (SELECT 
        jobLink,
            jobId,
            collectDate,
            jobTitle,
            salaryLow,
            jobSeniorityCategories,
            CASE
                WHEN salaryHigh < salaryLow THEN salaryLow
                ELSE salaryHigh
            END AS salaryHighAdj,
            LTRIM(SUBSTRING(companyName, LENGTH(jobTitle) + 2, LENGTH(companyName))) AS companyName,
            LTRIM(SUBSTRING(jobCategory, LENGTH('Kategoria: '), LENGTH(jobCategory))) AS jobCategory,
            CASE
                WHEN POSITION('UoP' IN salaryDetails) THEN 'Employment Contract'
                WHEN POSITION('na rękę' IN salaryDetails) THEN 'B2B'
                WHEN POSITION('B2B' IN salaryDetails) THEN 'B2B'
                ELSE 'OTHER'
            END AS employmentType
    FROM
        jobDescriptionTable
    LEFT JOIN jobSeniorityTable ON jobDescriptionTable.jobSeniority = jobSeniorityTable.jobSeniority
    WHERE
        salaryLow > 0
    LIMIT 100000) AS jobDescriptionTableFormat;

-- Update a key
ALTER TABLE jobDescriptionTableFormat
ADD PRIMARY KEY (jobId);

-- Export CSV
select * from jobDescriptionTableFormat;
select * from jobDescriptionTableFormat
INTO OUTFILE '/Users/grzegorzpopielnicki/Documents/GitHub/job-skill-recommender/temp.csv'
FIELDS ENCLOSED BY '"'
TERMINATED BY ';'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n';



-- Create view to matches both skills and job description, especially salary
DROP TABLE IF EXISTS describedSkillsTable;
CREATE TABLE describedSkillsTable AS
select
skillsTable.jobLink, skillsTable.jobId, skillsTable.skills, 
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


alter table SkillwithMainCategoryTable
ADD PRIMARY KEY (skills);


-- Prepare dictionary table for processing and categorisation
DROP TABLE IF EXISTS skillsDictionary;
CREATE TABLE skillsDictionary AS
SELECT 
    TEMP.skills,
    TEMP.NumSkills,
    SkillwithMainCategoryTable.jobCategory, skillsCategorised.skillsAgr
FROM
    (SELECT 
        REPLACE(skills, ';', '//') AS skills,
            COUNT(skills) AS NumSkills
    FROM
        describedSkillsTable
    GROUP BY skills) AS TEMP
        LEFT JOIN
    SkillwithMainCategoryTable ON TEMP.skills = SkillwithMainCategoryTable.skills 
		LEFT JOIN 
    skillsCategorised ON TEMP.skills = skillsCategorised.skills;
    

call mapSkills();
select * from skillsDictionarySQL
where ISNULL(skillsAgr);


-- 3/ match skills with aggregated skills
-- Replace ";" occurances to avoid join issues from csv
drop table if exists describedSkillsAgrTable;
create table describedSkillsAgrTable AS
select jobId, jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean,
jobSeniorityCategories, skillsDictionarySQL.skillsAgr,
concat(jobId, '_', row_number() over (partition by jobLink order by skillsAgr)) as jobId_skillNum
FROM (
select jobId, jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean,
jobSeniorityCategories, skills 
from describedSkillsTable) AS TEMP
left join skillsDictionarySQL 
on TEMP.skills = skillsDictionarySQL.skills;


ALTER TABLE describedSkillsAgrTable
MODIFY jobId_skillNum VARCHAR(100);

ALTER TABLE describedSkillsAgrTable 
ADD PRIMARY KEY (jobId_skillNum);

-- Export TABLE to CSV;



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




