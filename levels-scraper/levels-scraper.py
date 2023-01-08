from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
from tqdm import tqdm
import sys
sys.path.append('utils')
from utils import DataProcessor
from config import grouping_type, base_url
import numpy as np


class LevelsScraper:
    def __init__(self, base_url):
        """
        Initialize job list, Selenium Chrome Driver, BeautifulSoup, and DynamoDB objects
        """
        self.BASE_URL = base_url

        # Initialize main objects
        options = Options()
        options.headless = False
        self.driver = webdriver.Chrome('utils/chromedriver', options=options)
        self.dfs = []
        self.initializeWebsite()


    def initializeWebsite(self):
        """
        Go to website and close all popups / blurs
        Return the page source for pandas to parse
        :return:
        """
        # Navigate to the link
        self.driver.get(self.BASE_URL)

        # Wait till page loads
        WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, 
                    '//*[@id="__next"]/div/div[2]/div[1]/div/article/div/div[2]/a/p')
                    )
                )
        tqdm.write('-'*40)
        tqdm.write('page loaded')

        # exit blur overlays 
        self.exitBlur()

        # save number of pages to scrape
        self.num_pages = self.getNumPages()
    
    def exitBlur(self):
        """
        exit blur on site entry
        """
        try:
            exit_button = self.driver.find_element(
                by="xpath", 
                value = '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div[2]/table/tbody/tr[5]/td/div/div/button'
            )
            exit_button.click()
            exit_button_2 = self.driver.find_element(
                by="xpath", 
                value = '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div[2]/table/tbody/tr[5]/td/div/div/button'
            )
            exit_button_2.click()
        except:
            tqdm.write('blur not found')

    def getNumPages(self):
        # num_pages = int(self.driver.find_element(by="xpath",
        #     value = '/html/body/div[1]/div/div[2]/div[2]/div[2]/div[2]/table/tfoot/tr/td/div/div[1]',
        # ).text.split('of')[-1].strip().replace(',', '')) // 50
        num_pages = 1
        tqdm.write('-'*40)
        tqdm.write(f'num pages to scrape = {num_pages}')
        return num_pages   

    def cleanPageSource(self):
        """
        clean formatting from multi-info cells in table
        return: str (cannot edit page_source inplace)
        """
        self.delimiter = '|'

        cleaned_page_source = (self.driver.page_source
        ).replace('css-4cycqh">', f'css-4cycqh">{self.delimiter}'
        ).replace('css-ku77fz">', f'css-ku77fz">{self.delimiter}'
        ).replace('css-uh1cyf">', f'css-uh1cyf">{self.delimiter}'
        ).replace('css-1voc5jt">', f'css-1voc5jt">{self.delimiter}'
        )
        return cleaned_page_source

    def formatDf(self):
        '''
        remove invalid row entries and reformat multi-info cells
        into multiple columns
        '''
        # drop invalid rows
        self.df.drop(self.df.index[(
        (self.df[self.df.columns[0]].str.contains('1 submission is hidden')) | 
        (self.df[self.df.columns[0]].str.contains('Rows Per Page'))
        )], inplace=True)
        
        # separate multi-info cells

    def navigateToNextPage(self, page_num):
        # navigate to next url
        url = self.BASE_URL + f'&offset={50*page_num}'
        self.driver.get(url)

        # wait till page load
        WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, 
                    '//*[@id="__next"]/div/div[2]/div[1]/div/article/div/div[2]/a/p')
                    )
                )

    def scrapePages(self):
        progress_bar = tqdm(range(self.num_pages), position=0, leave=True)
        for page_num in progress_bar:
            progress_bar.set_description(f'page {page_num+1}')
            # clean page before parsing html in pandas
            cleaned_page_source = self.cleanPageSource()
            df = pd.read_html(cleaned_page_source, na_values=np.nan)[0]
            if len(df)<=2:
                tqdm.write('-'*40)
                tqdm.write(f'could not scrape data from page {page_num+1}')
            self.dfs.append(df)
            self.navigateToNextPage(page_num+1)

        # concat all scraped dfs into one df
        self.df = pd.concat(self.dfs)


class Processor(DataProcessor):
    def __init__(self, df, delimiter):
        super().__init__(df)
        self.df = df
        self.delimiter = delimiter
    
    def changeDfDtypes(self):
        '''
        change data types, especially for monetary columns
        '''
        monetary_cols = ['Total Compensation (USD)', 'Base', 'Stock (yr)', 'Bonus']
        for col in monetary_cols:
            series = self.df[col]
            series = series.str.replace('$', '').str.replace(',', '')
            series = series.str.replace('N/A', '0'
                    ).str.replace('K', '*1000'
                    ).str.replace('M', '*1000000'
                    ).str.replace('B', '*1000000000'
                    ).apply(lambda val : eval(val))
            self.df[col] = series

    def processData(self):
        # expand delimited columns
        self.separateDelimitedColumns()
        # remove invalid rows from ads
        self.cleanData()
        # change dtypes 
        self.changeDfDtypes()



    def separateDelimitedColumns(self):
        '''
        separates delimited columns into multiple columns
        reassigns new data to dataframe (class variable)
        assumes header is same format and length as rows, 
        hence some preprocessing required for edge cases
        eg. negotiated amount cells delimited but not accounted 
        for in header name
        '''
        # preprocess to rename header rows
        old_comp_header = 'Total Compensation (USD)|Base | Stock (yr) | Bonus'
        old_yoe_header = 'Years of Experience|Total / At Company'
        self.df.rename(columns={
            old_comp_header:f'Negotiated{self.delimiter}{old_comp_header}',
            old_yoe_header:f'Unnamed{self.delimiter}{old_yoe_header}' # dummy Unnamed to drop later
            }, inplace=True)

        new_df = []
        # format regular delimited columns
        for col_name in self.df.columns:
            expanded_df = self.df[col_name].str.strip().str.split(
                self.delimiter, expand=True)
            expanded_col_names = [
                new_col_name.strip() for new_col_name 
                in col_name.split(self.delimiter)]
            expanded_df.columns = expanded_col_names
            # drop Unnamed dummy columns 
            expanded_df.drop(columns='Unnamed', inplace=True, errors='ignore')

            if len(expanded_col_names) == 1:
                print(f'column {col_name} not expanded')
            new_df.append(expanded_df)
        
        # aggregate into single df
        self.df = pd.concat(new_df, axis=1)
        
        # fill nans generated in Negotiated column
        self.df['Negotiated'].fillna(0, inplace=True)
        print(self.df.isna().sum())
        

    def cleanData(self):
        '''
        remove invalid rows mainly from ads
        '''
        # TODO: replace with a more generalized dropna
        first_col = self.df[self.df.columns[0]]
        self.df = self.df[
            ~(first_col.str.contains('hidden to'
            ) | first_col.str.contains('Rows Per Page'
            ) | first_col.str.contains('your money back'
            ) | first_col.str.contains('Level up with'
            ) | first_col.str.contains('Review your resume'
            ))
        ]

if __name__ == '__main__':
    # scrape data and create df
    scraper = LevelsScraper(base_url)
    scraper.scrapePages()
    scraper.driver.close()

    # process df
    procsr = Processor(scraper.df, scraper.delimiter)
    procsr.processData()

    # group data
    procsr.groupRows(grouping_type=grouping_type, group_col='Company', order_col='Base')
    
    # save as csv if desired
    procsr.saveDf(save_path='levels-scraper/levels_data.csv')
    
    # push to Google Sheets
    procsr.pushDfToSheets(
        spreadsheet_key = '1KlgBLVdq0ZcpM5jol6sCQBR6dM1Nh0ZB23rYfJzkcdM',
        wks_name = 'levels-ml',
    )
