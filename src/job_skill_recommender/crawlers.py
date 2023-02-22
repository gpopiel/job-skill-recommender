import pandas as pd  # To get dataframe object
import requests  # To obtain page request
import mysql.connector  # To perform MySQL DB Operations
from bs4 import BeautifulSoup  # To fetch website data
import re  # Regular expressions
import pdb


class JobOfferScanner():
    def __init__(self, url, database, databaseTable, host, user, password):
        self.url = url
        page = requests.get(self.url)
        self.soup = BeautifulSoup(page.content, 'html.parser')
        self.df = None
        self.database = database
        self.databaseTable = databaseTable
        self.host = host
        self.user = user
        self.password = password
        self.cursor = None
        self.db = None

    def mySqlDatabaseConnect(self):
        '''Connects to mySQL and creates a cursor'''
        # Connect to mysql server
        self.db = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

        self.cursor = self.db.cursor(buffered=True)

    def remove_html_tags(self, string):
        """Remove all html tags from a string"""
        import re
        clean = re.compile('<.*?>')
        return re.sub(clean, '', string)

    def formatString(self, string):
        if isinstance(string, str):
            string = [string]
        string = [re.sub('[\[\]]', '', i) for i in string]
        string = [i.lstrip().rstrip() for i in string]
        string = list(filter(None, string))
        return string

    def replaceSpecialCharacters(self, stringList):
        """Check for special characters that should not be within SQL columns and replaces them with text """
        specialCharacters = [
            '#', '+', '.']  # List of special characters - to be modified on the go
        # List of corresponding replacements
        specialCharactersReplacements = ['sharp', 'plus', 'dot']
        # For every item on specialCharacters
        for i in range(len(specialCharacters)):
            # Check and replace every list item for special character that is iterated over
            # re.espace ensures that special characters do not throw an error
            stringList = [re.sub(re.escape(
                specialCharacters[i]), specialCharactersReplacements[i], item) for item in stringList]
        return stringList

    def selectElementByCSS(self, css_selector, separator=',', regex='r"\W+"'):
        """Obtains content, stripping unnecessary HTML tags"""
        selection = self.soup.select(css_selector)  # Get desired fragment
        selection = self.remove_html_tags(
            str(selection)).split(separator)  # Split by comma
        selection = [re.sub(regex, '', i) for i in selection]
        return selection

    def collectData(self):
        # Job ID
        jobId = {'jobId': self.url[self.url.rfind('-')+1:-1]}

        # Job TITLE
        jobTitle = self.selectElementByCSS('h1', regex=r'[\]\[]')[0]
        jobTitle = {'jobTitle': self.formatString(jobTitle)[0]}

        # SKILLS
        # skills = {'skills': '//'.join(getItem(soup, '.mb-0 .ng-star-inserted span',regex=r" |\[|\]"))}
        try:
            skills = self.selectElementByCSS(
                '.mb-0 .ng-star-inserted span', regex=r" |\[|\]")
            skills = self.replaceSpecialCharacters(skills)
            skills = {'SKL_'+item: 1 for item in skills}
        except:
            skills = {'SKL_NONE': 1}

        # SALARY
        try:
            salary = self.selectElementByCSS('.salary', regex=r"\xa0|\[|\]")[0]
            salaryRange = re.findall(r'\d+', salary)
            salaryDetails = salary[salary.find(re.findall(
                r'\d+', salary)[-1]) + len(re.findall(r'\d+', salary)[-1]):-1]
            salaryRange = self.formatString(salaryRange)
            salaryDetails = self.formatString(salaryDetails)[0]
            salaryRange = [float(num) for num in salaryRange]
            salary = {
                'salaryLow': salaryRange[0], 'salaryHigh': salaryRange[1], 'salaryDetails': salaryDetails}
        except:
            salary = {
                'salaryLow': -1, 'salaryHigh': -1, 'salaryDetails': -1}

        # JOB CATEGORY
        jobCategory = {'jobCategory': self.formatString(
            self.selectElementByCSS('aside', regex='[\]\[]')[0])[0]}

        # LEVEL
        jobSeniority = {'jobSeniority': "//".join(self.formatString(
            self.selectElementByCSS('#posting-seniority .ng-star-inserted')[0:2]))}

        # COMPANY NAME
        companyName = {'companyName': self.formatString(
            self.selectElementByCSS('#posting-header')[-1])[0]}

        # Szczegóły oferty
        try:
            additionalInfo = {'additionalInfo': "//".join(self.formatString(
                self.selectElementByCSS('#posting-specs')[0].split('  ')[1:-1]))}
        except:
            additionalInfo = {'additionalInfo': 'N/A'}

        # JOB METHODOLOGY
        try:
            jobMethodology = {'jobMethodology': self.formatString(
                self.selectElementByCSS('#posting-environment', regex='[\]\[]'))[0]}
        except:
            jobMethodology = {'jobMethodology': 'N/A'}

        # Job Details
        try:
            jobDetails = {'jobDetails': self.formatString(
                self.selectElementByCSS('#posting-specs', regex='[\]\[]'))[0]}
        except:
            jobDetails = {'jobDetails': 'N/A'}

        # Job - additional extras
        try:
            jobExtras = {'jobExtras': "//".join(self.formatString(
                self.selectElementByCSS('.purple .tw-mt-\[-10px\]')[0].split('  ')))}
        except:
            jobExtras = {'jobExtras': 'N/A'}

        # Job - benefits
        try:
            jobBenefits = {'jobBenefits': "//".join(self.formatString(
                self.selectElementByCSS('.success'))[0].split('  '))}
        except:
            jobBenefits = {'jobBenefits': 'N/A'}

        # Job - location
        try:
            jobLocation = {
                'jobLocation': "//".join(self.formatString(self.selectElementByCSS('.p-4'))[0].split('  '))}
        except:
            jobLocation = {'jobLocation': 'N/A'}
        # Sprzet
        try:
            equipment = self.formatString(self.selectElementByCSS(
                '#posting-equipment')[0].split())[1:-1]
            equipment = {"EQUIP_"+item: 1 for item in equipment}
        except:
            equipment = {'EQUIP_NONE': 1}

        self.df = pd.DataFrame([{**jobId, **jobTitle, **companyName, **jobCategory, **jobSeniority, **salary,
                                 **additionalInfo, **jobMethodology, **jobDetails, **jobExtras, **jobBenefits,
                                **jobLocation, **skills, **equipment}])

    def mySqlCreateNewColumnsIfNotExist(self):
        '''
        Dynamic creation of columns if they do not exists in sql DB yet
        cursor - cursor object from mysql.connector library
        Cols - an array of columns to check
        '''
        cols = [i for i in self.df.columns]  # Fetch All Column names
        # Prepare columns for a query;
        columnsToQuery = "`"+"`,`".join(cols)+"`"

        # Prepare a query
        query = f'SELECT * from {self.databaseTable} LIMIT 1;'
        self.cursor.execute(query)  # Execute
        databaseColumns = self.cursor.column_names  # Get ALL database column names

        # For every column in items
        counter = 0
        for item in cols:
            # Check if it is not already defined in the database
            if (item not in databaseColumns):
                # If it isn't - perform a query to add it
                self.cursor.execute(
                    f"ALTER TABLE {self.databaseTable} ADD COLUMN `{item}` TINYINT;")
                counter += 1
        print(f'{counter} columns inserted')

    def insertValuesIntoSqlDatabase(self):
        '''
        Inserts values in MySQL Database
        assumes prior connection to database using msql.connector method
        WARNING: throws an error on duplicate key (UPDATE TODO)
        database - database name
        databaseTable - database table to insert values to
        cursor - cursor object to execute queries
        df - dataframe containing values to be inserted
        '''

        cols = [i for i in self.df.columns]  # Fetch All Column names
        columnsToQuery = "`"+"`,`".join(cols)+"`"

        # Make a tuple containing values
        tuples = [tuple(x) for x in self.df.to_numpy()]
        # Build a query. It inserts values to corresponding columns that were found on db
        # TODO ON DUPLICATE KEY UPDATE jobId=jobId
        query = re.sub(
            "\[|\]", "",   f"INSERT INTO {self.databaseTable} ({columnsToQuery}) VALUES {tuples} ;")
        # Try executing the query
        try:
            self.cursor.execute(query)
            self.db.commit()
            print(self.cursor.rowcount, "record inserted.")
        except Exception as e:
            print(e)
            print(
                'Error - something went wrong with mySQL db udate.\n Check for duplicate id')


if __name__ == "__main__":
    print('Module')
