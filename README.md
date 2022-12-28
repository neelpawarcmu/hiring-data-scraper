# hiring-data-scraper
### levels fyi scraper
(in progress)

### h1b data scraper
Scrapes data from [h1data.info](https://h1bdata.info/) into a dataframe, parses unique company names and populates a google sheet with the data
To run this, follow the following
1. clone this repo using git clone `https://github.com/neelpawarcmu/hiring-data-scraper.git`
1. (optional) preferably install conda and create a conda env
1. `conda install pip`
2. install requirements using `pip install -r requirements.txt`
3. follow [this tutorial]('https://docs.gspread.org/en/latest/oauth2.html') to get a json file and add it to the `h1b-scraper` directory
4. run `python h1b-scraper/h1b-scraper.py`