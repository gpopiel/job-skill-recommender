-- JOBLINKS DATABASE INITIALIZATION AND LINK EXPORT

USE webScrap;

-- See basic attributes
describe jobLinkDataTable;

-- Get total number of rows
SELECT COUNT(*) FROM jobLinkDataTable;
-- 1st iteration: 2522
-- 2nd iteration: 2957

-- How many job offers per category?
SELECT 
	category, COUNT(category) AS numberOfJobs 
FROM 
	jobLinkDataTable
GROUP BY category
ORDER BY numberOfJobs DESC;

-- Create a table that stores all the duplicates (to check for potential errors)
DROP TABLE IF EXISTS duplicates;
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

select * from jobLinkDataTable;

-- Create a temporary table that would be fetched by python skill for links to investigate
DROP TABLE IF EXISTS linksToCollectTable;
CREATE TABLE linksToCollectTable AS
SELECT jobLink FROM (
	SELECT 
    jobLinkDataTable.jobId, jobLinkDataTable.jobLink, jobDescriptionTableFormat.jobTitle 
    FROM 
    jobLinkDataTable
	left join jobDescriptionTableFormat on jobLinkDataTable.jobId = jobDescriptionTableFormat.jobId) AS TEMP
WHERE isnull(jobTitle);



select * from jobDescriptionTableFormat;



