"""
TODO: Description
"""
from datetime import (datetime, date)

from utils.db import DbBase
from forex.pair import Pair
from forex.pair_package import PairPackage
from forex.to_parse_forex import ToParseForex


class DB(DbBase):
    """
    TODO: Description
    """
    _instance_ = None

    def __init__(self):
        super().__init__(__name__) # Logging name passed

    ##################
    ## Pair Package ##
    ##################

    def get_full_pair_package(self, exchangeDatetime: datetime):
        """Get a full pair package with all pairs on it"""
        if not isinstance(exchangeDatetime, (datetime, date)):
            raise TypeError(f"exchangeDatetime must be a datetime or date object."
                            f"Instead {type(exchangeDatetime)} was given")
        query_date = datetime(year=exchangeDatetime.year,
                              month=exchangeDatetime.month,
                              day=exchangeDatetime.day,
                              hour=0)

        self.logger.info("Getting full pair package with "
                        f"exchangeDate: {query_date.date()}")
        collection = self.database['forex_rates']
        result = collection.find_one(
            filter={"exchangeDate": query_date},
            projection={"_id": 0}
        )
        if result is None:
            self.logger.info("Pair Package was not found")
            return None
        self.logger.info("Found Pair Package")
        return PairPackage(**result)

    def get_pair_package(self, pair_package: PairPackage):
        """Get forex rate from forex_rates collection"""
        query_date = pair_package.exchangeDatetime()
        query_pairs = [Pair(base=p.base, quote=p.quote)
                       for p in pair_package.pairs]

        self.logger.info(f"Getting {len(query_pairs)} forex rates "
                         f"with date {pair_package.exchangeDate}")
        collection = self.database['forex_rates']
        result = collection.aggregate([
            {"$match": {"exchangeDate": query_date}},
            {"$unwind": "$pairs"},
            {"$addFields": {"value": "$pairs.value"}},
            {"$unset": "pairs.value"},
            {"$match": {"pairs": {"$in": query_pairs}}},
            {"$addFields": {"pairs.value": "$value"}},
            {"$unset": "value"},
            {"$group": {"_id": "$exchangeDate", "pairs": {"$push": "$pairs"}}},
            {"$project": {"_id": 0, "exchangeDate": "$_id", "pairs": 1}}
        ])
        if not result.alive:
            self.logger.warning(f"Found 0 forex rates instead of {len(query_pairs)} "
                                f"in {pair_package.exchangeDate}")
            return None
        result = PairPackage(**next(result)) # Must be only one result
        if len(result.pairs) != len(pair_package.pairs):
            self.logger.warning(f"Found {len(result.pairs)} forex rates instead of "
                                f"{len(query_pairs)} in {pair_package.exchangeDate}")
            return None # Does not have all pairs
        self.logger.info(f"Found {len(result.pairs)} forex rates")
        return result # Have all pairs

    def update_pair_package(self, pair_package: PairPackage):
        """Update an existing (otherwise, insert a new one) pair package"""
        if any("value" not in p for p in pair_package.pairs):
            raise ValueError("Lacking 'value' attribute, at least in 1 pair")
        self.logger.info(f"Updating pair package {pair_package.exchangeDate}")

        query_date = pair_package.exchangeDatetime()
        original = self.get_full_pair_package(query_date)
        collection = self.database['forex_rates']
        if original is None:
            self.logger.info("There is no previous pair package. Inserting to DB.")
            collection.insert_one(PairPackage(**pair_package)) # New Pair Package
            return

        self.logger.debug("Building pairs with new rates and adding new pairs")
        new_pairs = []
        pairs_to_parse = list(pair_package.pairs)
        for p in original.pairs:
            for index, p2 in enumerate(pairs_to_parse):
                temp_p2 = Pair(**p2)
                if temp_p2 not in new_pairs and\
                    p.base == p2.base and p.quote == p2.quote:
                    new_pairs.append(temp_p2) # New Value
                    pairs_to_parse.pop(index) # Remove used pair
                    break
            else:
                temp_p = Pair(**p)
                if temp_p not in new_pairs:
                    new_pairs.append(temp_p) # Old Value
        for p2 in pairs_to_parse:
            temp_p2 = Pair(**p2)
            if temp_p2 not in new_pairs:
                new_pairs.append(temp_p2) # Completely new pair

        self.logger.debug("Update to DB")
        collection.update_one(
            filter={"exchangeDate": query_date},
            update={"$set": {"pairs": new_pairs}}
        )
        self.logger.info(f"Pair Package updated with {len(new_pairs)} pairs")

    def replace_pair_package(self, pair_package: PairPackage):
        """Replace an existing pair package"""
        if any(key not in p
               for key in ["value", "base", "quote"]
               for p in pair_package.pairs):
            raise ValueError("Lacking 'value' attribute, at least in 1 pair")
        self.logger.info(f"Replacing forex rate: {pair_package.exchangeDate} with {len(pair_package.pairs)} pairs")
        query = {"exchangeDate": pair_package.exchangeDatetime()}
        collection = self.database['forex_rates']
        collection.replace_one(filter=query, replacement=pair_package, upsert=True)
        self.logger.info(f"Forex rate replaced: {pair_package.exchangeDate} with {len(pair_package.pairs)} pairs")


    ####################
    ## To Parse Forex ##
    ####################

    def get_to_parse_forex(self, to_parse_forex: ToParseForex):
        return self._get_to_process_(to_parse_forex)

    def insert_one_to_parse_forex(self, to_parse_forex: ToParseForex):
        self._insert_one_to_process_(to_parse_forex)

    def insert_many_to_parse_forex(self, to_parse_forex_list: list):
        self._insert_many_to_process_(to_parse_forex_list)

    def get_and_update_to_parse_forex(self, to_parse_forex: ToParseForex):
        return self._get_and_update_to_process_(to_parse_forex)

    def get_and_lock_first_to_parse_forex(self):
        return self._get_and_lock_first_to_process_("to_parse_forex")

    def remove_lock_to_parse_forex(self, dead_time_seconds: int):
        self._remove_lock_to_process_("to_parse_forex", dead_time_seconds)

    def count_waiting_to_parse_forex(self):
        return self._count_waiting_to_process_("to_parse_forex")

    def count_pending_revision_to_parse_forex(self):
        return self._count_pending_revision_to_process_("to_parse_forex")

    def count_locked_to_parse_forex(self):
        return self._count_locked_to_process_("to_parse_forex")
