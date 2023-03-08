'''
SCRIPT
BROWSES NOFLUFFJOBS MAJOR CATEGIRES AND SAVES LINKS TO TARGET JOB OFFERS
SAVES RESULTS TO DATABASE
'''

from bs4 import BeautifulSoup  # To fetch website data
import requests  # To collect a request
import time  # To setup a sleep function
import random  # To setup a random variable for sleeping break between scrapes
import pandas as pd  # To build a DataFrame object
import mysql.connector

import re  # Regex operations

# PART ONE - FETCH ALL JOB LINKS, BUILD A SIMPLE DICTIONARY

# # Build a list of categories
categories = ['backend', 'frontend', 'fullstack', 'mobile',
              'testing', 'devops', 'embedded', 'architecture', 'security', 'gaming',
              'artificial-intelligence', 'big-data', 'support', 'it-administrator', 'agile', 'product-management',
              'project-manager', 'business-intelligence', 'business-analyst',
              'ux', 'erp', 'sales', 'marketing', 'backoffice', 'hr', 'other']


# # Assume search for up to 100 pages
paginations = list(range(1, 100))
progress = 0  # Set progress
iterations = len(categories) * len(paginations)

# Assume constant page root
pageRoot = 'https://nofluffjobs.com/pl/'


# Initialize empty dataframe
df = pd.DataFrame([{
    'category': None,
    'pagination': None,
    'currentSearchPage': None,
    'jobPositionCounter': None,
    'jobLink':  None,
    'jobId': None
}])

# For every category in categories array
for category in categories:
    jobPositionCounter = 0  # Reset jobPosition when entering a new category
    # For every pagination in paginations array
    for pagination in paginations:
        jobPositionCounter += 1000  # Increment job Position by 100
        # Build search link that would yield jobs
        # Current search page
        currentSearchPage = f'{pageRoot}{category}?page={pagination}'
        # Connect to the page
        page = requests.get(currentSearchPage)
        soup = BeautifulSoup(page.content, 'html.parser')
        # Store all links in a variable
        soupLinks = soup.find_all('a', href=True)
        # Extract hrefs
        soupLinks = [i['href'] for i in soupLinks]
        jobSearchString = 'pl/job/'
        # If no new job links, pass to next category
        if len([i for i in soupLinks if jobSearchString in i]) == 0:
            print(f'No more jobs to find in {category}')
            break
        # FOR EVERY HREF
        for link in soupLinks:
            jobPositionCounter += 1  # Increment job position counter by one
            # If it matches the job offer pattern
            if jobSearchString in link:
                jobUrl = link  # Store link in a temporary variable
                jobId = jobUrl[jobUrl.rfind('-')+1:-1]  # Get JobId;
                df.loc[len(df)] = [category, pagination, currentSearchPage,
                                   jobPositionCounter, jobUrl, jobId]  # Add  a row to DataFrame
        randum = random.randint(3, 30)  # Determine sleep length
        # Display progress
        print(
            f'Progress: Category {category}. Page {pagination}. Sleeping for {randum} seconds')
        time.sleep(randum)

# PART TWO - CONNECT TO DATABASE AND SEND THE DATA, OMMITING DUPLICATES

# INPUT DB CREDENTIALS
database = 'webScrap'
databaseTable = 'jobLinkDataTable'
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

cursor = db.cursor(buffered=True)  # Set up cursor for db operations
# Fetch all the collected job ids
query = f'select jobLink, jobId from {databaseTable};'
# Execute the query, build the pandas dataframe
jobsSearchedIds = pd.read_sql(query, db)
jobsSearchedIds = jobsSearchedIds.jobId.values.tolist()  # Convert ids to list
# Keep only the rows that are not present in the database
df = df[~df['jobId'].isin(jobsSearchedIds)]


# Unique offer id, regardless of category


# Fetch column names as an array
cols = [i for i in df.columns]  # Fetch All Column names
# Build a query-ready part of column names
columnsToQuery = "`"+"`,`".join(cols)+"`"

# DATA  INSERTION
# For every record in dataframe, starting second (first is None placeholder)
for i in range(2, len(df)+1):
    # Make a tuple containing values
    tuples = [tuple(x) for x in df.iloc[i-1:i].to_numpy()]
    # Build a query. It inserts values to corresponding columns that were found on db
    # Duplicates are ignored
    query = re.sub(
        "\[|\]", "",   f"INSERT INTO {databaseTable}({columnsToQuery}) VALUES {tuples} ON DUPLICATE KEY UPDATE jobId=jobId;")
    # Try executing the query
    print(f'{i} / {len(df)} iteration')
    try:
        cursor.execute(query)
        db.commit()
        print(cursor.rowcount, "record inserted.")
    except Exception as e:
        print(e)
        print(
            'Error - something went wrong with mySQL db udate.')
