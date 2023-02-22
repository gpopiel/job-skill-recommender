# import pandas as pd
# import seaborn as sns
# import mysql.connector
# print('import successful')
import pdb


from src.job_skill_recommender.crawlers import JobOfferScanner
database = 'webScrap'
databaseTable = 'scrapDataTable'


url = 'https://nofluffjobs.com/pl/job/angular-frontend-developer-automotive-next-technology-professionals-remote-b2ndyta6'

website = JobOfferScanner(url=url,
                          database=database,
                          databaseTable=databaseTable,
                          host='localhost',
                          user='root',
                          password='12344321')
website.collectData()
website.mySqlDatabaseConnect()
website.mySqlCreateNewColumnsIfNotExist()
website.insertValuesIntoSqlDatabase()
