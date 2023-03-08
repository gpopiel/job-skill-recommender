'''
Loops through all jobs and collects data that is immediately saved to MySQL database
'''

import sys
import random  # To setup a random variable for sleeping break between scrapes
import time
import mysql.connector
from src.job_skill_recommender.crawlers import JobOfferScanner
from src.job_skill_recommender.databaseFunctions import mySqlDatabaseConnect
from src.job_skill_recommender.databaseFunctions import insertValuesIntoSqlDatabase


# # GET LINKS THAT NEED TO BE COLLECTED
# INPUT DB CREDENTIALS
database = 'webScrap'
databaseTable = 'linksToCollectTable'
# databaseTable = 'scrapDataTable'
host = 'localhost'
user = 'root'
password = '12344321'

# Connect to database
db = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = db.cursor()  # Set up a cursor
cursor.execute(f'SELECT * FROM {databaseTable}')  # Execute query

jobLinks = cursor.fetchall()  # Get query results, stored as list (1 column)
root = 'https://nofluffjobs.com'  # Root part of every link

# JOB OFFER COLLECTION
database = 'webScrap'
# databaseTable = 'scrapDataTable'


# INITIALIZE THE CLASS
website = JobOfferScanner()

progress = 0  # Set up p    rogress to be 0
numberOfJobs = len(jobLinks)
startTime = time.time()  # Record start time

url = root+jobLinks[1][0]
website.collectData(url)  # Collect website data
err_id = []
err_log = []
# # For every job left to be scanned
for job in jobLinks:
    # Build an url
    url = root+job[0]  # Get next URL
    website.collectData(url)  # Collect website data
    # Append Database columns for new skills
    try:
        insertValuesIntoSqlDatabase(
            website.dfjobDescription, 'jobDescriptionTable', db)
        insertValuesIntoSqlDatabase(website.dfskills, 'skillsTable', db)
        insertValuesIntoSqlDatabase(website.dfequipment, 'equipmentTable', db)
    except Exception as e:
        print(e)
        err_id.append(job[0])
        err_log.append(e)
     # Insert values to SQL database
    randum = random.randint(3, 20)  # Determine sleep length
    # Display progress
    progress += 1
    print(
        f'Progress: {progress} / {numberOfJobs} ({round(progress / numberOfJobs,2)*100}%). Runtime: {round((time.time() - startTime)/60,0)}min. Sleeping for {randum} seconds')
    time.sleep(randum)
