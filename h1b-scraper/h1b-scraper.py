# Import libraries
import pandas as pd
from config import roles, companiesToExclude, grouping_type
import sys
sys.path.append('utils')
from utils import DataProcessor
from tqdm import tqdm


class H1bScraper:
    def __init__(self, roles) -> None:
        # Create an URL object
        self.roles = roles
        self.generateUrlsFromRoleNames()
    
    def generateUrlsFromRoleNames(self):
        self.urls = [
            'https://h1bdata.info/index.php?em=&job='+'+'.join(
                role.lower().split())+'&city=&year=all+years' 
            for role in self.roles
        ]

    def scrape(self):
        self.generateDfFromURLs()

    def generateDfFromURLs(self):
        '''
        generate and store df from urls
        '''
        progress_bar = tqdm(self.urls, position=0, leave=True)
        dfs = [pd.read_html(url)[0] for url in progress_bar if url]
        self.df = pd.concat(dfs)

class Processor(DataProcessor):
    def __init__(self, df):
        super().__init__(df)

    def changeDfDtypes(self):
        '''
        change dtypes using a mapping for col names
        '''
        dtypes = {
            'numeric_cols': ['BASE SALARY'],
            'date_cols': []
        }
        numeric_cols = dtypes['numeric_cols']
        self.df[numeric_cols] = self.df[numeric_cols
            ].apply(pd.to_numeric, errors='coerce', axis=1
            ).astype(int, errors='ignore')

    def cleanDf(self, ref_col):
        '''
        drop extra columns and nan rows
        drop blacklisted companies
        '''
        # drop 'Unnamed' cols from processing
        self.df.drop(self.df.columns[self.df.columns.str.match("Unnamed")], axis=1, inplace=True)
        # drop nan rows
        self.df.dropna(axis=0, inplace=True)
        # remove blacklisted companies 
        self.df.drop(self.df[self.df[ref_col].isin(companiesToExclude)].index, inplace=True)

    def processDf(self, group_col='EMPLOYER', order_col='BASE SALARY'):
        '''
        filter out and sort results
        '''
        # change dtypes from str
        self.changeDfDtypes()

        # clean df nans and blacklisted companies
        self.cleanDf(ref_col='EMPLOYER')

        # group employers
        self.groupRows(grouping_type=grouping_type, group_col=group_col, order_col=order_col)


if __name__ == "__main__":
    scraper = H1bScraper(roles)
    scraper.scrape()

    procsr = Processor(scraper.df)
    procsr.processDf()

    procsr.pushDfToSheets(
        spreadsheet_key = '1KlgBLVdq0ZcpM5jol6sCQBR6dM1Nh0ZB23rYfJzkcdM',
        wks_name = 'h1b-data',
    )

    
    