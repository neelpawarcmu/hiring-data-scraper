import gspread
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm
import pandas as pd

class DataProcessor:
    def __init__(self, df):
        self.df = df
            
    def pushDfToSheets(self, spreadsheet_key, wks_name):
        '''
        push df to google sheets using the sheets API. 
        Refer to README.md for instructions on use
        follows this tutorial: 
        https://towardsdatascience.com/using-python-to-push-your-pandas-dataframe-to-google-sheets-de69422508f
        corrects some of the bugs in the tutorial using stackoverflow
        '''
        # authorize google sheets api
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'utils/google-sheet-key.json', scope
        )
        gc = gspread.authorize(credentials)

        # reset index to avoid d2g problems: see stack overflow
        # https://stackoverflow.com/questions/54833419/python-df2gspread-library-can-not-save-a-df-to-google-sheet
        self.df.reset_index(inplace=True)

        # upload df to sheets
        d2g.upload(self.df, spreadsheet_key, wks_name, credentials=credentials, row_names=False)

    def downloadSheetsToDf(self, spreadsheet_key, wks_name):
        scope = ['https://spreadsheets.google.com/feeds', 
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'utils/google-sheet-key.json', scope
        )
        self.df = g2d.download(gfile = spreadsheet_key,
            wks_name = wks_name,
            col_names = True, row_names = True,
            credentials = credentials
        )

    def groupRows(self, grouping_type, group_col, order_col=None, avg_col=None):
        '''
        group as one of the foll: repeated, multiindex or by employer average salary
        Note: gspread upload doesnt support multiindex currently, hence built a workaround
        '''
        if grouping_type=='multiindex':
            # only reorder if required
            # if order_col: 
            #     self.df.sort_values(order_col, ascending=False, inplace=True)
            
            # group employer names, preserving sorted order
            self.df = pd.concat(
                [self.df[self.df[group_col] == cmp] for cmp in self.df[group_col].unique()]
            )

            # blank out repeated names 
            seen_names = set()
            for i, company in enumerate(self.df[group_col]):
                if company in seen_names:
                    self.df.iloc[i, :][group_col] = ''
                else:
                    seen_names.add(company)
        
        elif grouping_type == 'mean':
            assert not avg_col, 'argument avg_col cannot be None'
            self.df = self.df.groupby(group_col, sort=False).agg({avg_col:'mean'})

        # self.df.set_index([self.df[group_col], self.df[order_col]], inplace=True)
        # self.df.sort_index(level=1, inplace=True, ascending=False)
        # self.df.drop(columns=[group_col, order_col], inplace=True)
        # if grouping_type == 'repeat':
        #     self.df.reset_index(inplace=True, col_level=1)
        # elif grouping_type == 'first':
        #     self.df.reset_index(inplace=True)
        #     self.df = self.df.groupby(group_col, sort=False, ).agg('first')
        # elif grouping_type == 'multiidx':
        #     raise NotImplementedError
            
    def saveDf(self, save_path):
        '''
        save composed df
        '''
        self.df.to_csv(save_path, sep='\t', index=False, lineterminator='\n')
        tqdm.write('-'*40)
        tqdm.write('converted to csv')