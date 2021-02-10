from .constants import VALID_CURRENCIES

def generate_default_pairs():
    return [Pair(base=base, quote=quote)
            for base in VALID_CURRENCIES
            for quote in VALID_CURRENCIES
            if base != quote]

def generate_custom_pairs(currencies_list):
    return [Pair(base=base, quote=quote)
            for base in currencies_list
            for quote in currencies_list
            if base != quote]

class Pair(dict):
    """Foreign exchange pair"""
    def __init__(self, base, quote, value=None):
        super().__init__()
        self.base = base
        self.quote = quote
        if value:
            self.value = value

    def __setattr__(self, name, value):
        if name not in dir(self):
            raise AttributeError(f"'{name}' is not an attribute of Pair")
        super().__setattr__(name, value)

    @property
    def base(self):
        return self["base"]
    @base.setter
    def base(self, value):
        if not isinstance(value, str):
            raise TypeError(f"Value is not a str. Instead is {type(value)}")
        if value not in VALID_CURRENCIES:
            raise ValueError(f"Value is not a valid currency: {value}")
        self["base"] = value
    @base.deleter
    def base(self):
        del self["base"]

    @property
    def quote(self):
        return self["quote"]
    @quote.setter
    def quote(self, value):
        if not isinstance(value, str):
            raise TypeError(f"Value is not a str. Instead is {type(value)}")
        if value not in VALID_CURRENCIES:
            raise ValueError(f"Value is not a valid currency: {value}")
        self["quote"] = value
    @quote.deleter
    def quote(self):
        del self["quote"]

    @property
    def value(self):
        return self["value"]
    @value.setter
    def value(self, value):
        if isinstance(value, int):
            value = float(value)
        if not isinstance(value, float):
            raise TypeError(f"Value is not int or float. Instead is {type(value)}")
        self["value"] = round(value, ndigits=5) # Store up to 5 decimals
    @value.deleter
    def value(self):
        del self["value"]

class PairList(list):
    """Unique pairs inside this list"""
    def __init__(self, *args):
        super().__init__()
        for arg in args:
            for element in arg:
                self.append(element)

    def append(self, value: Pair):
        if isinstance(value, dict):
            value = Pair(**value)
        for index, pair in enumerate(self):
            if pair.base == value.base and pair.quote == value.quote:
                self[index] = Pair(**{**pair, **value})
                break
        else:
            super().append(Pair(**value))
