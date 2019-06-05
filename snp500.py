import bs4 as bs  # beautiful soup
import datetime as dt
import os
import pandas as pd
import pandas_datareader.data as web
import pickle  # serializes any python object
import requests

def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text  # get the first column
        tickers.append(ticker)

    with open('sp500tickers.pickle', 'wb') as f:
        pickle.dump(tickers, f)

    print(tickers)

    return tickers


# save_sp500_tickers()

def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open('sp500tickers.pickle', 'rb') as f:
            tickers = pickle.load(f)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    start = dt.datetime(2000, 1, 1)
    end = dt.datetime(2016, 12, 31)

    for ticker in tickers:
        ticker = ticker[:-1]  # Scraping from wikipedia returns 'MMM/n' to the pickle file.
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            if ticker != 'BBT' and ticker != 'BRK.B' and ticker != 'BF.B' and ticker != 'CF' and ticker != 'CTVA'\
                    and ticker != 'COTY' and ticker != 'DOW' and ticker != 'DD' and ticker != 'EW'\
                    and ticker != 'EVRG' and ticker != 'HIG' and ticker != 'LIN' and ticker != 'OKE'\
                    and ticker != 'PNW' and ticker != 'PPG' and ticker != 'DGX' and ticker != 'VMC'\
                    and ticker != 'ZBH':
                df = web.DataReader(ticker, 'yahoo', start, end)
                df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))

    print('completed')

get_data_from_yahoo()