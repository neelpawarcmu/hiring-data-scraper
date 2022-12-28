# Import libraries
import pandas as pd
from config import urls, companiesToExclude
import gspread
import df2gspread as d2g


class Scraper:
    def __init__(self, urls) -> None:
        # Create an URL object
        self.urls = urls
    
    def scrape(self):
        self.generateDfFromURLs()
        self.processDf()
        print(self.df)

    def generateDfFromURLs(self):
        '''
        generate and store df from urls
        '''
        dfs = [pd.read_html(url)[0] for url in self.urls]
        self.df = pd.concat(dfs)

    def changeDfDtypes(self):
        '''
        change dtypes by mapping
        '''
        dtypes = {
            'numeric_cols': ['BASE SALARY'],
            'date_cols': []
        }
        numeric_cols = dtypes['numeric_cols']
        self.df[numeric_cols] = self.df[
            numeric_cols
        ].apply(pd.to_numeric, errors='coerce', axis=1)

    def cleanDf(self):
        '''
        drop extra columns and nan rows
        drop blacklisted companies
        '''
        # drop 'Unnamed' cols from processing
        self.df.drop(self.df.columns[self.df.columns.str.match("Unnamed")], axis=1, inplace=True)
        # drop nan rows
        self.df.dropna(axis=0, inplace=True)
        # remove blacklisted companies 
        self.df.drop(self.df[self.df.EMPLOYER.isin(companiesToExclude)].index, inplace=True)

    def processDf(self, order='BASE SALARY'):
        '''
        filter out and sort results
        '''
        # change dtypes from str
        self.changeDfDtypes()

        # clean df nans and blacklisted companies
        self.cleanDf()
        
        # change dtypes
        # group by company and order by salary
        self.df.sort_values('BASE SALARY', ascending=False, inplace=True)
        self.df.set_index([self.df['EMPLOYER'], self.df['BASE SALARY']], inplace=True)
        self.df.sort_index(level=0, inplace=True)

if __name__ == "__main__":
    scraper = Scraper(urls)
    scraper.scrape()