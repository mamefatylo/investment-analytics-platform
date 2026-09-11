"""
exchange_mapping.py

Correspondance entre les places de cotation
du benchmark iShares et les codes d'échange
utilisés par OpenFIGI.

Cette table est utilisée lors de l'acquisition
du Security Master (SRC-002).
"""

EXCHANGE_MAPPING = {
    "NASDAQ": "US",
    "NYSE": "US",
    "SIX Swiss Exchange": "SW",
    "London Stock Exchange": "LN",
    "Hong Kong Exchanges And Clearing Ltd": "HK",
    "Tokyo Stock Exchange": "JP",
    "Taiwan Stock Exchange": "TT",
    "Toronto Stock Exchange": "CN",
    "Shanghai Stock Exchange": "CH",
    "Shenzhen Stock Exchange": "SZ",
    "Singapore Exchange": "SP",
    "National Stock Exchange Of India": "IN",
    "Korea Exchange (Stock Market)": "KS",
    "Korea Exchange (Kosdaq)": "KQ",
    "Xetra": "GY",
    "Bolsa De Madrid": "SM",
    "Bolsa Mexicana De Valores": "MM",
    "Oslo Bors Asa": "NO",
    "Nasdaq Omx Nordic": "SS",
    "Omx Nordic Exchange Copenhagen A/S": "DC",
    "Nyse Euronext - Euronext Paris": "FP",
    "Nyse Euronext - Euronext Brussels": "BB",
    "Nyse Euronext - Euronext Lisbon": "PL",
    "Wiener Boerse Ag": "AV",
    "Johannesburg Stock Exchange": "SJ",
    "Tel Aviv Stock Exchange": "IT",
    "Saudi Stock Exchange": "AB",
    "Borsa Italiana": "IM",
    "Asx - All Markets": "AU",
}