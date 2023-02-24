# SCRIPT
# BROWSERS NOFLUFFJOBS MAJOR CATEGIRES AND SAVES LINKS TO TARGET JOB OFFERS
# SAVES RESULTS TO DATABASE

from bs4 import BeautifulSoup  # To fetch website data
import requests  # To collect a request
import time  # To setup a sleep function
import random  # To setup a random variable for sleeping break between scrapes
import pandas as pd  # To build a DataFrame object
import mysql.connector  # To connect and send data to DB
import re  # Regex operations

# PART ONE - FETCH ALL JOB LINKS, BUILD A SIMPLE DICTIONARY

# # Build a list of categories
categories = ['backend', 'frontend', 'fullstack', 'mobile',
              'testing', 'devops', 'embedded', 'architecture', 'security', 'gaming',
              'ai', 'big-data', 'support', 'it-administrator', 'agile', 'product-management',
              'project-manager', 'business-intelligence', 'business-analyst',
              'design', 'erp', 'sales', 'marketing', 'backoffice', 'hr', 'other']

# # Assume search for 10 pages
paginations = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
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
        # FOR EVERY HREF
        for a in soup.find_all('a', href=True):
            jobPositionCounter += 1  # Increment job position counter by one
            # If it matches the job offer pattern
            if "pl/job/" in a['href']:
                jobUrl = a['href']  # Store href in temporary variable

                jobId = category + '-' + \  # Get JobId;
                jobUrl[jobUrl.rfind('-')+1:-1]  # Obtain unique jobId
                df.loc[len(df)] = [category, pagination, currentSearchPage,
                                   jobPositionCounter, jobUrl, jobId]  # Add  a row to DataFrame
        randum = random.randint(3, 30)  # Determine sleep length
        progress += 1  # increment progress
        # Display progress
        print(
            f'Progress: {round(progress/iterations,2)}. Category {category}. Page {pagination}. Sleeping for {randum} seconds')
        time.sleep(randum)

# PART TWO - CONNECT TO DATABASE AND SEND THE DATA, OMMITING DUPLICATES

# INPUT DB CREDENTIALS
database = 'webScrap'
databaseTable = 'jobLinkDataTable'
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

cursor = db.cursor(buffered=True)  # Set up cursor for db operations


# TODO - INCLUDE COLUMN IN FUTURE LOOPS
# Unique offer id, regardless of category
df.jobUniqueId = df.jobId.str.replace('^(.*-)', '')


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
