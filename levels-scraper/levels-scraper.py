from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.conditions import Key
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import sendgrid
import os
from sendgrid.helpers.mail import *
from twilio.rest import Client
from lxml import etree
from configs import num_pages


class JobScraper:
    def __init__(self):
        """
        Initialize job list, Selenium Chrome Driver, BeautifulSoup, and DynamoDB objects
        """
        self.URL = 'https://www.levels.fyi/t/software-engineer/focus/ml-ai?countryId=254&country=254&limit=50'
        self.num_pages = num_pages

        # Initialize main objects
        options = Options()
        options.headless = True
        self.driver = webdriver.Chrome('./chromedriver', options=options)
        self.dfs = []
        self.initialize_website()

    def initialize_website(self):
        """
        Go to website and close all popups / blurs
        Return the page source for pandas to parse
        :return:
        """
        # Navigate to the link
        self.driver.get(self.URL)

        # Wait till page loads
        WebDriverWait(
            self.driver, 20
            ).until(
                EC.presence_of_element_located(
                    (By.XPATH, 
                    '//*[@id="__next"]/div/div[2]/div[1]/div/article/div/div[2]/a/p')
                    )
                )
        print('-'*40)
        print('page loaded')

        # exit blur overlays 
        self.exit_blur()
        self.scrape_pages()
        self.driver.close()

    def clean_page(self):
        """
        clean formatting before parsing as df
        return: str
        """
        return self.driver.page_source#.replace('css-ku77fz">', 'css-ku77fz"> | ')

    def exit_blur(self):
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
            print('blur not found')
            return

    def scrape_pages(self):
        for page in range(self.num_pages)[:1]: # TODO: Remove
            cleaned_page = self.clean_page()
            df = pd.read_html(cleaned_page)[0]
            if len(df)<=2:
                print('*'*40)
                print(f'page {page+1} not loaded before df conversion')
            self.dfs.append(df)
            next_button = self.driver.find_element(
                by="xpath", 
                value='//*[@id="__next"]/div/div[2]/div[2]/div[2]/div[2]/table/tfoot/tr/td/div/div[2]/div/button[6]'
                )
            next_button.click()
            WebDriverWait(self.driver, 30)
        self.df = pd.concat(self.dfs)
        print('-'*40)
        print(self.df)
        self.df.to_csv('levels_data.csv', sep='\t')
        print('-'*40)
        print('converted to csv')
        return


if __name__ == '__main__':
    js = JobScraper()
    # js.scrape_pages()
