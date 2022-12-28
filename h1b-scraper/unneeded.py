# Import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
from config import urls, companiesToExclude


class Scraper:
    def __init__(self, urls, table_id) -> None:
        # Create an URL object
        self.urls = urls
        self.table_id = table_id
    
    def scrape(self):
        self.getTablesFromURLs()
        self.tablesToDf()
        self.processDf()
        print(self.df)

    def getTablesFromURLs(self):
        '''
        returns:  -> list[bs4.element.Tag]
        '''
        self.tables = [self.urlToTable(url) for url in self.urls]

    def tablesToDf(self):
        # Obtain every title of columns with tag <th>
        headers = []
        for i in self.tables[0].find_all('th'):
            title = i.text
            headers.append(title)
        # init dataframe
        self.df = pd.DataFrame(columns = headers)
        # populate df with all tables' data
        for table in self.tables:
            self.addTableToDf(table)
            

    def urlToTable(self, url): #REED
        '''
        parses a single url to page data
        '''
        # Create object page
        page = requests.get(url)
        # parser-lxml = Change html to Python friendly format
        # Obtain page's information
        soup = BeautifulSoup(page.text, 'lxml')
        table = soup.find('table', { 'id' : self.table_id })
        return table

    def addTableToDf(self, table):
        '''
        parse data in a table and add all its rows to df
        '''
        for j in table.find_all('tr')[1:]:
            # find table entry and add to df
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(self.df)
            try:
                self.df.loc[length] = row
            except:
                print('row mismatch')
                continue

    def processDf(self, order='BASE SALARY'):
        '''
        filter out and sort results
        '''
        # remove blacklisted companies 
        self.df.drop(self.df[self.df.EMPLOYER.isin(companiesToExclude)].index, inplace=True)
        # group by company and order by salary
        self.df.sort_values('BASE SALARY', ascending=False, inplace=True)
        self.df.set_index([self.df['EMPLOYER'], self.df['BASE SALARY']], inplace=True)
        self.df.sort_index(level=0, inplace=True)
        

if __name__ == "__main__":
    scraper = Scraper(urls, table_id="myTable")
    scraper.scrape()