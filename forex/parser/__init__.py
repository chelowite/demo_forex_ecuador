"""
Source parsers
"""
import datetime
import requests

from forex.pair import Pair
from forex.pair_package import PairPackage

class GenericParser():
    """Abstraction layer for a specific forex web"""
    def __init__(self):
        self.base_currency = None
    def _make_url(self, currencies: list, date: datetime.date) -> str:
        """Builds api url"""
        raise NotImplementedError
    def _get_currency_data(self, url: str) -> dict:
        """Makes api call and returns JSON content"""
        raise NotImplementedError
    def _parse_data(self, json_data: dict, pair_package: PairPackage) -> PairPackage:
        """Parse JSON content and stores rates values in each pair of pair_package"""
        raise NotImplementedError
    def pull(self, pair_package: PairPackage) -> PairPackage:
        """Get rates from forex site given a pair_package"""
        raise NotImplementedError
