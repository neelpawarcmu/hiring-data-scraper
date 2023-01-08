roles = [
    role for role in open(
        'h1b-scraper/config-role-names.txt', 'r').read().splitlines() 
        if role
    ]

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

grouping_type = 'repeat'