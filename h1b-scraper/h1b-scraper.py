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
        self.generateDfFromURLs()
        self.processDf()
        # print(self.df)

    def generateDfFromURLs(self):
        '''
        generate and store df from urls
        '''
        dfs = [pd.read_html(url)[0] for url in self.urls]
        self.df = pd.concat(dfs)
        print(self.df)

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