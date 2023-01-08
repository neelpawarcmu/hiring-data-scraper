# hiring-data-scraper
Scrapes current hiring data from levels.fyi and h1bdata.info. To run any scraper in this repo, follow the following preliminary steps and then follow specific steps under the scraper of interest.
1. clone this repository using git clone `https://github.com/neelpawarcmu/hiring-data-scraper.git`
1. (optional) preferably install conda and create a conda env
1. `conda install pip`
1. change active directory to cloned repo using `cd hiring-data-scraper`
1. install requirements using `pip install -r requirements.txt`

### levels fyi scraper
Scrapes data from [levels.fyi](https://www.levels.fyi/) into a dataframe, parses unique company names and populates a google sheet with the data.

### h1b data scraper
Scrapes data from [h1data.info](https://h1bdata.info/) into a dataframe, parses unique company names and populates a google sheet with the data.
1. follow [this tutorial]('https://docs.gspread.org/en/latest/oauth2.html') to get a json file, rename it to `google-sheet-key.json` and add it to the `hiring-data-scraper/utils` directory
1. change the `spreadsheet_key` and `wks_name` in `h1b-scraper.py` bottom of code
1. make desired changes to `config-role-names.txt` and `config.py` 
1. run `python h1b-scraper/h1b-scraper.py`