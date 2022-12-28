# Import libraries
import pandas as pd
from config import urls, companiesToExclude, grouping
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials


class Scraper:
    def __init__(self, urls) -> None:
        # Create an URL object
        self.urls = urls
    
    def scrape(self):
        self.generateDfFromURLs()
        self.processDf()

    def generateDfFromURLs(self):
        '''
        generate and store df from urls
        '''
        dfs = [pd.read_html(url)[0] for url in self.urls if url]
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
        self.df[numeric_cols] = self.df[numeric_cols
            ].apply(pd.to_numeric, errors='coerce', axis=1
            ).astype(int, errors='ignore')

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
        # reset index to avoid d2g problems: see stack overflow
        # https://stackoverflow.com/questions/54833419/python-df2gspread-library-can-not-save-a-df-to-google-sheet
        self.df.reset_index(inplace=True)

    def groupEmployers(self, grouping):
        '''
        group as one of teh foll: repeated, multiindex or by employer average salary
        '''
        # order by salary
        # condense employer names to single indices
        self.df.set_index([self.df['EMPLOYER'], self.df['BASE SALARY']], inplace=True)
        self.df.sort_index(level=1, inplace=True, ascending=False)
        self.df.drop(columns=['EMPLOYER', 'BASE SALARY'], inplace=True)
        if grouping == 'repeat':
            self.df.reset_index(inplace=True)
        elif grouping == 'first':
            self.df.reset_index(inplace=True)
            self.df = self.df.groupby('EMPLOYER', sort=False, ).agg('first').reset_index()
        elif grouping == 'multiidx':
            return

    def processDf(self, order='BASE SALARY'):
        '''
        filter out and sort results
        '''
        # change dtypes from str
        self.changeDfDtypes()

        # clean df nans and blacklisted companies
        self.cleanDf()
        
        # group employers
        self.groupEmployers(grouping=grouping)

    def pushDfToSheets(self):
        '''
        follows this tutorial: 
        https://towardsdatascience.com/using-python-to-push-your-pandas-dataframe-to-google-sheets-de69422508f
        corrects some of the bugs in the tutorial using stackoverflow
        '''
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'google-sheet-key.json', scope)
        gc = gspread.authorize(credentials)
        spreadsheet_key = '1KlgBLVdq0ZcpM5jol6sCQBR6dM1Nh0ZB23rYfJzkcdM'
        wks_name = 'Sheet1'
        d2g.upload(self.df, spreadsheet_key, wks_name, credentials=credentials, row_names=False)

if __name__ == "__main__":
    scraper = Scraper(urls)
    scraper.scrape()
    scraper.pushDfToSheets()
    