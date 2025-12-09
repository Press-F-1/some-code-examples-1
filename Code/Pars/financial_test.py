#!/usr/bin/env python3

import sys
import time
import requests
import pytest

from bs4 import BeautifulSoup


def souper(ticker='', fild=''):
    values = []
    try:
        time.sleep(5)
        
        url = f'https://finance.yahoo.com/quote/{ticker.upper()}/financials/?p={ticker.lower()}'
        
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

        response = requests.get(url, headers=headers)
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.find_all('div', class_='yf-t22klz')

        target_row = None
        for row in rows:
            title_span = row.find('div', title=fild)
            if title_span and title_span.text == fild:
                target_row = row
                break


        if target_row != None:    
            value_cells = target_row.find_all('div', class_='column')
            for cell in value_cells:
                if cell.text.strip():
                    values.append(cell.text.strip())
            values = tuple(values)
        else:
            values = 'Wrong argument'

    except:
        raise AttributeError('Wrong argument')


    return values


def test_souper1():
    assert type(souper('MSFT', 'Total Revenue')) == tuple

def test_souper2():
    with pytest.raises(AttributeError) as ex_info:
        souper(6)
    assert str(ex_info.value) == 'Wrong argument'

def test_souper3():
    assert souper('fofofofoofof', 'Cost of Revenue') == 'Wrong argument'


if __name__ == '__main__':
    if len(sys.argv) == 3:

        ticker = sys.argv[1]
        fild = sys.argv[2]
        
        result = souper(ticker, fild)

    else:
        result = 'Wrong argument'

    print(result)

