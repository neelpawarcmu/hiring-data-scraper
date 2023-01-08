import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm

class DataProcessor:
    def __init__(self, df):
        self.df = df
            
    def pushDfToSheets(self, spreadsheet_key, wks_name):
        '''
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

    def groupRows(self, grouping_type, group_col, order_col):
        '''
        group as one of the foll: repeated, multiindex or by employer average salary
        '''
        # order by salary
        # condense employer names to single indices
        self.df.set_index([self.df[group_col], self.df[order_col]], inplace=True)
        self.df.sort_index(level=1, inplace=True, ascending=False)
        self.df.drop(columns=[group_col, order_col], inplace=True)
        if grouping_type == 'repeat':
            self.df.reset_index(inplace=True, col_level=1)
        elif grouping_type == 'first':
            self.df.reset_index(inplace=True)
            self.df = self.df.groupby(group_col, sort=False, ).agg('first')
        elif grouping_type == 'multiidx':
            raise NotImplementedError
            
    def saveDf(self, save_path):
        '''
        save composed df
        '''
        self.df.to_csv(save_path, sep='\t', index=False, lineterminator='\n')
        tqdm.write('-'*40)
        tqdm.write('converted to csv')