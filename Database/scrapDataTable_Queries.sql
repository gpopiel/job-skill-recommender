-- PHASE TWO: COLLECTED JOB OFFERS DATA PROCESSING

use webScrap; -- Database name

-- Basic queries for database connectivity and columns
describe skillsTable;
describe jobDescriptionTable;
describe equipmentTable;
describe jobLinkDataTable;

select * from skillsTable LIMIT 1;  -- Raw skills table, long format
select * from jobDescriptionTable LIMIT 10; -- Raw job dimensions table
select * from equipmentTable LIMIT 1; -- Raw equipment table, long format
select * from jobLinkDataTable LIMIT 1; -- Raw joblink table (sources the collected jobs)

-- How many skill records
select count(*) from skillsTable;

-- REMOVE DUPLICATES by using partition by clause -- Store in a new table
CREATE TABLE IF NOT EXISTS skillsTableNoDuplicates AS
SELECT * FROM (
	SELECT * ,
	row_number() over (partition by jobXSkills) as duplicate_count
	FROM (
		SELECT *, CONCAT(jobId, skills) AS jobXSkills
		FROM skillsTable) as skillsTable_withNewColumn
	) AS duplicateSearchTable
WHERE duplicate_count=1;

-- UNTIL DELTA IS IMPLEMENTED, FK CONSTRAINED REMOVED
-- TODO, REWRITE FOR DELTA
ALTER TABLE describedSkillsAgrTable
DROP FOREIGN KEY describedskillsagrtable_ibfk_1;

-- Clean the job description table, extract some features (READY BI TABLE)
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
-- TODO: DELTA 
ALTER TABLE jobDescriptionTableFormat
ADD PRIMARY KEY (jobLink);

-- DIMENSION AND FACT INTEGRATION:
-- Create table that matches both skills and job description, especially salary
DROP TABLE IF EXISTS describedSkillsTable;
CREATE TABLE describedSkillsTable AS
select skillsTableNoDuplicates.skillId,
skillsTableNoDuplicates.jobLink, skillsTableNoDuplicates.jobId, skillsTableNoDuplicates.skills,
jobCategory, salaryLow, salaryHighAdj, salaryMean, jobSeniorityCategories
from 
skillsTableNoDuplicates
left join jobDescriptionTableFormat  on skillsTableNoDuplicates.jobLink = jobDescriptionTableFormat.jobLink
where salaryLow > 0;

-- TEMPORARY TABLE TO CLEAN UP SKILL TAGS
-- PREPARE a unique set up skill TAGS
-- AS an addtion, paste in category with highest frequency for a given skill
DROP TABLE IF EXISTS skillsDictionary;
CREATE TABLE skillsDictionary AS
		WITH added_row_number AS (
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY skills ORDER BY N DESC) AS irow_number
			FROM (
				SELECT skills, jobCategory, count(skills) as N
				from describedSkillsTable
				group by jobCategory, SKILLS
				order by jobCategory, N DESC
			) as tempTABLE2
				group by skills, jobCategory
			) 
		SELECT *
		FROM added_row_number
		WHERE irow_number = 1;

-- Make basic column modification
UPDATE skillsDictionary
SET skills = REPLACE(skills, ';', '//');

-- map skills to clean them up (use dictionary procedure)
call mapSkills();
DROP TABLE IF EXISTS skillsDictionary;

UPDATE skillsDictionarySQL
SET skillsAgr = UPPER(skillsAgr);

-- FINAL RESULT OF SKILLS TABLE
-- 3/ match skills with aggregated skills
drop table if exists describedSkillsAgrTable;
create table describedSkillsAgrTable AS
	select skillId, jobId, jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean, TEMP.skills,
	jobSeniorityCategories, skillsDictionarySQL.skillsAgr,
	concat(jobId, '_', row_number() over (partition by jobLink order by skillsAgr)) as jobId_skillNum
	FROM (
		select skillId, jobId, jobLink, jobCategory,salaryLow, salaryHighAdj, salaryMean,
		jobSeniorityCategories, skills 
		from describedSkillsTable) AS TEMP
	left join skillsDictionarySQL 
	on TEMP.skills = skillsDictionarySQL.skills;
    

-- ADD PRIMARY AND FOREIGN KEYS
-- TODO IN DETLTA FK WILL BE GIVEN
ALTER TABLE describedSkillsAgrTable 
ADD PRIMARY KEY (skillId);

ALTER TABLE describedSkillsAgrTable ADD FOREIGN KEY (jobLink)
REFERENCES jobDescriptionTableFormat(jobLink);

ALTER TABLE describedSkillsAgrTable ADD FOREIGN KEY fk_skills (skills)
REFERENCES skillsDictionarySQL(skills);

ALTER TABLE equipmentTable ADD FOREIGN KEY fk_jobLink (jobLink)
REFERENCES jobDescriptionTable (jobLink);

ALTER TABLE skillsTable ADD FOREIGN KEY fk_jobLink (jobLink)
REFERENCES jobDescriptionTable (jobLink);

ALTER TABLE describedSkillsAgrTable ADD FOREIGN KEY fk_jobLink (jobLink)
REFERENCES jobDescriptionTable (jobLink);

ALTER TABLE describedSkillsAgrTable ADD FOREIGN KEY (skillId)
REFERENCES skillsTable (skillId);

-- DROP UNUSED TABLES
DROP TABLE skillsTableNoDuplicates;
DROP TABLE describedSkillsTable;

-- FINAL TABLES FOR BI
select * from describedSkillsAgrTable; -- SKILLS with key features
select * from jobDescriptionTableFormat; -- Ready table to process


-- *** FURTHER ANALYSIS IN TABLEAU **** --
