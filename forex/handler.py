"""
Main module of Forex Handler
"""
from utils.to_process import INSERTED
from forex.db import DB
from forex.to_parse_forex import ToParseForex
from forex.pair_package import (PairPackage, PairPackageList)

class Handler():
    """
    Forex Handler. This can be used to get forex rates of
    exchange Pairs or also to insert to_parse_forex which
    indicates that some forex rates are lacking in DB
    """
    def __init__(self):
        self.db = DB()

    def get_one_forex_rate_value(self, pair_package: PairPackage) -> float:
        """Get a value of one exchange pair"""
        if not isinstance(pair_package, PairPackage):
            raise TypeError("pair package is not a PairPackage object, "
                            f"instead is {type(pair_package)}")
        if len(pair_package.pairs) != 1:
            raise ValueError("There must be 1 pair in pairs. Lenght of pairs: "
                            f"{len(pair_package.pairs)}")
        result = self.db.get_pair_package(pair_package)
        if result is None:
            return None
        return result.pairs[0].value

    def get_pair_package_values(
                self, pair_package: PairPackage) -> (PairPackage, None):
        """Return another PairPackage with all pairs having values"""
        if not isinstance(pair_package, PairPackage):
            raise TypeError("pair package is not a PairPackage object, "
                            f"instead is {type(pair_package)}")
        return self.db.get_pair_package(pair_package)

    def build_and_insert_one_to_parse_forex(
        self,
        pair_package: PairPackage,
        **kwargs
    ) -> None:
        """Build one to_parse_forex from a pair package and insert it in DB"""
        if "recordStatus" not in kwargs:
            kwargs["recordStatus"] = INSERTED
        self.db.insert_one_to_parse_forex(
            ToParseForex(pairPackage=pair_package, **kwargs)
        )

    def build_and_insert_many_to_parse_forex(
        self,
        pair_package_list: PairPackageList,
        **kwargs
    ) -> None:
        """Build many to_parse_forex from a pair package list and insert them in DB"""
        if isinstance(pair_package_list, list):
            pair_package_list = PairPackageList(pair_package_list)
        if not isinstance(pair_package_list, PairPackageList):
            raise TypeError("Pair package list is neither a list or a"
                            "PairPackageList. Instead is"
                            f"{type(pair_package_list)}")
        if "recordStatus" not in kwargs:
            kwargs["recordStatus"] = INSERTED
        to_upload = [ToParseForex(pairPackage=p, **kwargs)
                    for p in pair_package_list]
        self.db.insert_many_to_parse_forex(to_upload)
