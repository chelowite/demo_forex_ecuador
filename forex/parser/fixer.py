"""
Fixer.io parser
"""
import datetime
import requests

from forex.pair import Pair
from forex.pair_package import PairPackage
from . import GenericParser
from .errors import (ParserError, QuotaExceeded)

class Parser(GenericParser):
    """fixer.io abstraction layer"""
    def __init__(self):
        super().__init__()
        self.base_currency = 'EUR' # Fixed and cannot change

    def _make_url(self, currencies, date):
        """Builds api url"""
        BASE_URL = 'http://data.fixer.io/api/'
        ACCESS_KEY = '77bed253bd686730b95ed8d881f1d370'
        date = 'latest' if date == datetime.date.today() else date.strftime("%Y-%m-%d")
        currencies_query = ','.join(currencies) # Joins all currencies, e.g. ARS,EUR,GBP

        url = f"{BASE_URL}{date}?access_key={ACCESS_KEY}&symbols={currencies_query}"
        # url += "&format=1" # format json response: https://currencylayer.com/documentation#format
        return url

    def _get_currency_data(self, url):
        """Makes api call and returns JSON content"""
        response = requests.get(url)
        if response.status_code != 200:
            raise ConnectionError(f"Fixer connection error. Status code: {response.status_code}")
        return response.json()

    def _parse_data(self, json_data, pair_package):
        """Parse JSON content and stores rates values in each pair of pair_package"""
        rates = json_data['rates']
        for pair in pair_package.pairs:
            if pair.base == self.base_currency:
                pair.value = rates[pair.quote]
            else:
                pair.value = rates[pair.quote] / rates[pair.base]
        return pair_package

    def pull(self, pair_package):
        """Get rates from forex site given a pair_package"""
        if not isinstance(pair_package, PairPackage):
            raise TypeError("pair_package is not a PairPackage")
        for p in pair_package.pairs:
            if not isinstance(p, Pair):
                raise TypeError(f"pair is not a Pair object. Instead is {type(p)}")
            if 'quote' not in p or 'base' not in p:
                raise ValueError(f"Pair does not have quote or base currency: {p}")

        currencies_list = set(p.base for p in pair_package.pairs)
        currencies_list.update(p.quote for p in pair_package.pairs)

        url = self._make_url(currencies_list, pair_package.exchangeDate)
        json_data = self._get_currency_data(url)

        if not json_data['success']: # Something failed
            error_code = json_data['error']['code']
            error_info = json_data['error']['info']
            msg = f"Error code {error_code}. {error_info}"
            if error_code == 104:
                raise QuotaExceeded(msg)
            raise ParserError(msg)
        return self._parse_data(json_data, pair_package)
