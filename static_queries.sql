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

select * from jobSeniorityTable;

ALTER TABLE jobSeniorityTable ADD PRIMARY KEY (jobSeniority);
