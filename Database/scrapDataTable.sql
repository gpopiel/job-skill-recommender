-- JOBLINKS DATABASE INITIALIZATION AND LINK EXPORT

USE webScrap;

-- CREATE EMPTY DATABASE
create table webScrap.jobLinkDataTable (
jobId VARCHAR (100) PRIMARY KEY,
jobUniqueId VARCHAR (100),
category VARCHAR (100),
pagination TINYINT,
currentSearchPage VARCHAR (250),
jobPositionCounter smallint,
jobLink VARCHAR (500)
);

-- See basic attributes
describe jobLinkDataTable;

-- Get total number of rows
SELECT COUNT(*) FROM jobLinkDataTable;

-- How many job offers per category?
SELECT 
	category, COUNT(category) AS numberOfJobs 
FROM 
	jobLinkDataTable
GROUP BY category
ORDER BY numberOfJobs DESC;

-- Create a table that stores all the duplicates (to check for potential errors)
DROP TABLE duplicates;
CREATE TABLE duplicates
SELECT 
    jobUniqueId,jobLink,
    COUNT(jobUniqueId) as numberOfDuplicates
FROM
    jobLinkDataTable
GROUP BY jobUniqueId,jobLink
HAVING COUNT(jobUniqueId) > 1;

-- Check for duplicates on job ids
SELECT 
jobLinkDataTable.jobLink, jobLinkDataTable.category, duplicates.numberOfDuplicates
FROM 
jobLinkDataTable
INNER JOIN duplicates
ON jobLinkDataTable.jobUniqueId = duplicates.jobUniqueId;

-- Create a table of links to be used for procedure of link collection
CREATE TABLE jobLinksTable
SELECT DISTINCT jobLink from jobLinkDataTable
LIMIT 999999;
