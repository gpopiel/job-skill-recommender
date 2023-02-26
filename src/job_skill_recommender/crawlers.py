import pandas as pd  # To get dataframe object
import re  # Regular expressions
from bs4 import BeautifulSoup  # To fetch website data
import mysql.connector  # To perform MySQL DB Operations
import requests  # To obtain page request
from datetime import date


class JobOfferScanner():
    def __init__(self):
        # self.url = url
        self.dfjobDescription = None
        self.dfskills = None
        self.dfequipment = None
        self.soup = None

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

    def collectData(self, url):
        page = requests.get(url)
        self.soup = BeautifulSoup(page.content, 'html.parser')

        # Job ID
        jobId = {'jobId': url[url.rfind('-')+1:-1]}
        jobLink = {'jobLink': url[len('https://nofluffjobs.com'):-1]}

        # Job TITLE
        jobTitle = self.selectElementByCSS('h1', regex=r'[\]\[]')[0]
        jobTitle = {'jobTitle': self.formatString(jobTitle)[0]}

        # SKILLS
        # skills = {'skills': '//'.join(getItem(soup, '.mb-0 .ng-star-inserted span',regex=r" |\[|\]"))}
        try:
            skills = self.selectElementByCSS(
                '.mb-0 .ng-star-inserted span', regex=r" |\[|\]")
            skills = self.replaceSpecialCharacters(skills)
            skills = {'skills': skills}

            # skills = {'SKL_'+item: 1 for item in skills}
        except:
            skills = {'skills': 'N/A'}

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
        try:
            jobCategory = {'jobCategory': self.formatString(
                self.selectElementByCSS('aside', regex='[\]\[]')[0])[0]}
        except:
            jobCategory = {'jobCategory': 'N/A'}
        # LEVEL
        try:
            jobSeniority = {'jobSeniority': "//".join(self.formatString(
                self.selectElementByCSS('#posting-seniority .ng-star-inserted')[0:2]))}
        except:
            jobSeniority = {'jobSeniority': "N/A"}

        # COMPANY NAME
        try:
            companyName = {'companyName': self.formatString(
                self.selectElementByCSS('#posting-header')[-1])[0]}
        except:
            companyName = {'companyName': 'N/A'}
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

        # Job Requirements
        try:
            jobRequirements = {'jobRequirements': self.formatString(
                self.selectElementByCSS('#posting-requirements', regex='[\]\[]'))[0]}
        except:
            jobRequirements = {'jobRequirements': 'N/A'}

        # Job Description
        try:
            jobDescription = {'jobDescription': self.formatString(
                self.selectElementByCSS('#posting-description', regex='[\]\[]'))[0]}
        except:
            jobDescription = {'jobDescription': 'N/A'}

        # Job Description
        try:
            jobTasks = {'jobTasks': self.formatString(
                self.selectElementByCSS('#posting-tasks', regex='[\]\[]'))[0]}
        except:
            jobTasks = {'jobTasks': 'N/A'}

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
            equipment = {'equipment': equipment}
        except:
            equipment = {'equipment': 'N/A'}

        collectDate = {'collectDate': date.today().strftime("%d/%m/%Y")}

        # Create dataframe instance joining all the fields
        self.dfjobDescription = pd.DataFrame([{**jobId, **jobLink, **collectDate, **jobTitle, **companyName, **jobCategory, **jobSeniority, **salary,
                                               **additionalInfo, **jobMethodology, **jobRequirements, **jobDescription, **jobTasks,
                                               **jobDetails, **jobExtras, **jobBenefits,
                                               **jobLocation}])

        skillCount = len(skills['skills'])  # Number of Sklills
        self.dfskills = pd.DataFrame([
            [jobId['jobId']]*skillCount, [jobLink['jobLink']]*skillCount, skills['skills']]).T
        self.dfskills.columns = ['jobId', 'jobLink', 'skills']

        equipmentCount = len(equipment['equipment'])  # Number of equipment
        self.dfequipment = pd.DataFrame([
            [jobId['jobId']]*equipmentCount, [jobLink['jobLink']]*equipmentCount, equipment['equipment']]).T
        self.dfequipment.columns = ['jobId', 'jobLink', 'equipment']
