use webScrap;
-- DROP TABLE scrapDataTable;
-- INITIALIZE THE TABLE
-- CREATE TABLE webScrap.jobDescriptionTable (
-- 	jobLink VARCHAR (400) PRIMARY KEY,
--     jobId VARCHAR (100) ,
--     collectDate VARCHAR (50),
-- 	jobTitle VARCHAR (400),
-- 	companyName VARCHAR (400),
-- 	jobCategory VARCHAR (400),
-- 	jobSeniority VARCHAR (400),
-- 	salaryLow INTEGER,
-- 	salaryHigh INTEGER,
-- 	salaryDetails VARCHAR (600),
-- 	additionalInfo VARCHAR (1000),
-- 	jobMethodology VARCHAR (1000),
-- 	jobRequirements VARCHAR (1500),
-- 	jobDescription VARCHAR (1500),
-- 	jobTasks VARCHAR (1500),
-- 	jobDetails VARCHAR (1500),
-- 	jobExtras VARCHAR (500),
-- 	jobBenefits VARCHAR (500),
-- 	jobLocation VARCHAR (500)
-- );

-- CREATE TABLE webScrap.equipmentTable (
-- 	equipmentId int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
-- 	jobLink VARCHAR (400),
--     jobId VARCHAR (100),
--     equipment VARCHAR (250)
-- );

-- CREATE TABLE webScrap.skillsTable (
-- 	skillId int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
-- 	jobLink VARCHAR (400),
--     jobId VARCHAR (100),
--     skills VARCHAR (250)
-- );

-- alter table skillsTable
-- modify column skillId bigint NOT NULL AUTO_INCREMENT;

-- alter table equipmentTable
-- modify column equipmentId bigint NOT NULL AUTO_INCREMENT;



describe skillsTable;
select * from skillsTable;
select * from jobDescriptionTable;
select * from equipmentTable;


