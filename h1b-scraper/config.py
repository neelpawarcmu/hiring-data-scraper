urls = open('h1b-scraper/config-urls.txt', 'r').read().splitlines() 

companiesToExclude = [
    "APPLE INC",
    "META PLATFORMS INC",
    "TIKTOK INC",
    "AIRBNB INC",
    "FACEBOOK INC",
    "ADOBE INC",
    "SALESFORCECOM INC",
    "SPOTIFY USA INC",
    "DOORDASH INC",
]

grouping = 'first'