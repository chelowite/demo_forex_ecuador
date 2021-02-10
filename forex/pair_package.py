from datetime import (datetime, date)
from dateutil import parser

from .pair import (PairList, generate_default_pairs, generate_custom_pairs)

def generate_pair_package_with_default_pairs(exchange_date):
    return PairPackage(exchangeDate=exchange_date, pairs=generate_default_pairs())

def generate_pair_package_with_custom_pairs(exchange_date, currencies_list):
    return PairPackage(exchangeDate=exchange_date,
                       pairs=generate_custom_pairs(currencies_list))

class PairPackage(dict):
    """
    Foreign exchange pair package.
    Includes exchange date and a list of pairs of that date.
    """
    def __init__(self, exchangeDate, pairs=None):
        super().__init__()
        self.exchangeDate = exchangeDate
        if pairs:
            self.pairs = pairs
        else:
            self.pairs = []

    def __setattr__(self, name, value):
        if name not in dir(self):
            raise AttributeError(f"'{name}' is not an attribute of PairPackage")
        super().__setattr__(name, value)

    def __str__(self):
        msg = f"{self.exchangeDate} ["
        for pair in self.pairs:
            msg += f"{pair.base}/{pair.quote}"
            if "value" in pair:
                msg += f"/{pair.value}"
            msg += ","
        msg = msg[:-1] + "]" # Removes last comma
        return msg

    def exchangeDatetime(self) -> datetime:
        """Return exchangeDate as datetime"""
        return self["exchangeDate"]

    def exchangeDateStr(self) -> str:
        """Return exchangeDate as a string formatted as YYYY-MM-DD"""
        return self.exchangeDate.strftime("%Y-%m-%d") # for example "2020-12-01"

    @property
    def exchangeDate(self):
        return self["exchangeDate"].date()
    @exchangeDate.setter
    def exchangeDate(self, value):
        if isinstance(value, str):
            value = parser.parse(value).date()
        if isinstance(value, datetime):
            value = value.date()
        if not isinstance(value, date):
            raise TypeError(f"Value is not a date. Instead is {type(value)} / {value}")
        self["exchangeDate"] = datetime(value.year, value.month, value.day, 0)
    @exchangeDate.deleter
    def exchangeDate(self):
        del self["exchangeDate"]

    @property
    def pairs(self):
        return self["pairs"]
    @pairs.setter
    def pairs(self, value):
        if isinstance(value, list):
            value = PairList(value)
        if not isinstance(value, PairList):
            raise TypeError(f"Value is not a PairList. Instead is {type(value)}")
        self["pairs"] = value
    @pairs.deleter
    def pairs(self):
        del self["pairs"]

class PairPackageList(list):
    """Unique pair packages inside this list"""
    def __init__(self, *args):
        super().__init__()
        for arg in args:
            for element in arg:
                self.append(element)

    def append(self, value: PairPackage):
        if isinstance(value, dict):
            value = PairPackage(**value)
        for index, pair_package in enumerate(self):
            if pair_package.exchangeDate == value.exchangeDate:
                self[index].pairs = PairList(self[index].pairs, value.pairs)
                break
        else:
            super().append(PairPackage(**value))
