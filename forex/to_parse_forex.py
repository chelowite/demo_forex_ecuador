"""
TODO: Description
"""
from utils.to_process import ToProcess
from forex.pair_package import PairPackage

class ToParseForex(ToProcess):
    """
    TODO: Description
    """
    def __init__(self, **kwargs):
        super().__init__()
        for name, value in kwargs.items():
            setattr(self, name, value)

    @property
    def pairPackage(self):
        return self["pairPackage"]
    @pairPackage.setter
    def pairPackage(self, value: str):
        if isinstance(value, dict):
            value = PairPackage(**value)
        if not isinstance(value, PairPackage):
            raise TypeError(f"Value is not a PairPackage. Instead is: {type(value)}")
        self["pairPackage"] = value
    @pairPackage.deleter
    def pairPackage(self):
        del self["pairPackage"]
