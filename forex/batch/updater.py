"""
Main module of the Forex Batch Updater
"""
import importlib
from time import sleep
from datetime import (date, datetime, timedelta)
import argparse

import environment_config as config
from utils.batch import BatchBase
from utils.last_exec import LastExec

from forex.db import DB
from forex.constants import (VALID_SOURCES, VALID_CURRENCIES)
from forex.parser.errors import (ParserError, QuotaExceeded)
from forex.pair_package import (generate_pair_package_with_default_pairs,
                                generate_pair_package_with_custom_pairs)

def valid_date(s):
    """Date validation for argparse"""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date() # Return the date part
    except ValueError:
        msg = f"Not a valid date: '{s}''"
        raise argparse.ArgumentTypeError(msg)

def valid_currency_list(s: str):
    """Currency list validation for argparse"""
    currency_list = [e.strip() for e in s.split(",")]
    if all(currency in VALID_CURRENCIES for currency in currency_list):
        return currency_list
    msg = f"Not a valid currency list: '{s}'"
    raise argparse.ArgumentTypeError(msg)

BATCH_ARGUMENTS = [
    {
        "option_string": ["source"],
        "type": str,
        "choices": VALID_SOURCES,
        "help": "source name for parsing"
    },
    {
        "option_string": ["instance"],
        "type": str,
        "help": "Internal process identifier for Forex Batch Updater"
    },
    {
        "option_string": ["--start-date", "-s"],
        "type": valid_date,
        "help": "Start Date - format YYYY-MM-DD (default is today)"
    },
    {
        "option_string": ["--days-ago", "-d"],
        "type": int,
        "default": 0,
        "help": "Days to go backwards (default is zero)"
    },
    {
        "option_string": ["--currencies-list"],
        "type": valid_currency_list,
        "help": "List format is 'c1, c2, c3'"
                f"Use any of these currencies: {VALID_CURRENCIES}"
    }
]


# Constants
SLEEP_TIME = 6*3600 # seconds
QUOTA_EXCEEDED_SLEEP_TIME = 24*3600 # seconds

def generate_pair_package_with_custom_pairs_lambda(currencies_list):
    return lambda exchange_date:\
        generate_pair_package_with_custom_pairs(exchange_date, currencies_list)

class Batch(BatchBase):
    """
    Forex Batch Updater
    ---
    This batch is intended to pull historic data of forex
    or also constantly update with new forex rates every
    day.
    """
    def __init__(self, **kwargs):
        super().__init__(__name__)

        self.good_counter = 0
        self.errors_counter = 0
        self.woke_from_nap_and_start_working = False

        self.source = kwargs["source"]
        self.start_date = kwargs["start_date"]
        self.days_ago = kwargs["days_ago"]
        self.currencies_list = kwargs["currencies_list"]

        # Historic mode is activated when start_date is not None
        self.historic_mode = bool(kwargs["start_date"])

        # Sleep forced with this variable set to True
        # should be used to stop warning a lot of messages
        # when reached a point where should wait
        self.force_sleep = False

        # Sleep time when force sleeping
        self.force_sleep_time = None

        try:
            self.db = DB()
        except RuntimeError as e:
            error_msg = f"ERROR. Aborting process: {e}"
            self.notificator.send_message(error_msg)
            self.logger.exception(error_msg)
            raise

        self.last_exec = LastExec(
            __name__,
            param=kwargs['instance'],
            server=LastExec.get_server_name()
        )

        _ = kwargs # Not in use, but will raise error if it isn't in parameters

    def get_parser(self, source_name: str):
        """Get a parser for a specific source"""
        module_name = f"forex.parser.{source_name}"
        source_module = importlib.import_module(module_name)
        return source_module.Parser()

    def start_parsing(self, parser, pair_package):
        """
        Begin parsing pair package
        """
        self.logger.info("Start parsing")
        if config.DEBUG_MODE is True:
            # Parse without catching errors (interrupts at the first error)
            pair_package = parser.pull(pair_package)
            self.db.update_pair_package(pair_package)
            self.logger.info("Parse succeeded!")
            self.good_counter += 1
        else:
            # Parse catching errors (if it fails, it continues anyways)
            try:
                error_msg = None
                exd = pair_package.exchangeDate
                pair_package = parser.pull(pair_package)
                self.db.update_pair_package(pair_package)
                self.logger.info("Parse succeeded!")
                self.good_counter += 1
            except QuotaExceeded as e:
                msg = f"{self.source} Quota Exceeded: {e}"
                self.logger.warning(msg)
                self.notificator.send_message(msg)
                self.force_sleep = True
                self.force_sleep_time = QUOTA_EXCEEDED_SLEEP_TIME
            except ParserError as e:
                error_msg = f"Parser error. {exd} unable to complete: {e}"
            except ValueError as e:
                error_msg = f"Value error. {exd} unable to complete: {e}"
            except TypeError as e:
                error_msg = f"Type error. {exd} unable to complete: {e}"
            except (SystemError, RuntimeError) as e:
                error_msg = f"Fatal and UNEXPECTED error. {exd} unable "\
                            f"to complete: {e}"
            except Exception as e:
                error_msg = f"Exception OOOPS!. {exd} unable to complete: {e}"
            finally:
                if error_msg is not None:
                    self.logger.error(error_msg)
                    self.notificator.send_message(error_msg)
                    self.errors_counter += 1
        self.logger.info("Parse finished")

    def start_working(self):
        """Start indefinite batch loop"""
        self.logger.info("Initializing")

        # Dates initialization
        current_date = self.start_date if self.historic_mode else date.today()
        end_date = current_date - timedelta(days=self.days_ago)

        # Pair Package generator function initialization
        if self.currencies_list:
            generate_pair_package = generate_pair_package_with_custom_pairs_lambda(
                self.currencies_list
            )
        else:
            generate_pair_package = generate_pair_package_with_default_pairs

        # Parser initialization
        parser = self.get_parser(self.source)

        self.logger.info("Starting indefinite loop")
        while True:
            if current_date < end_date:
                if self.historic_mode:
                    self.logger.info("Work finished. Exiting.")
                    break
                self.last_exec.save() # Save last execution
                msg = f"Reseting counters (OK {self.good_counter}, "\
                        f"ERROR {self.errors_counter}) and current_date"
                self.logger.info(msg)
                current_date = date.today()
                end_date = current_date - timedelta(days=self.days_ago)
                self.good_counter, self.errors_counter = 0, 0
                msg = f"Work finished, taking a nap... ({SLEEP_TIME/3600} hs)"
                self.logger.info(msg)
                sleep(SLEEP_TIME)

            self.logger.info(f"Parse pair package with exchange date {current_date}")
            pair_package = generate_pair_package(current_date)

            if self.db.get_pair_package(pair_package):
                self.logger.info("All pairs are already in the DB. Skipped parsing")
                self.good_counter += 1
            else:
                self.start_parsing(parser, pair_package)

            if self.force_sleep:
                msg = "Force sleeping. Current date and counters "\
                     f"will not be modified ({self.force_sleep_time/3600} hs)"
                self.logger.info(msg)
                sleep(self.force_sleep_time)
                self.force_sleep = False
                self.force_sleep_time = None
            else:
                current_date = current_date - timedelta(days=1)

    def main(self):
        """External call to this batch to start operating"""
        msg = f"Starting {__name__} | Debug: {config.DEBUG_MODE}"
        self.logger.info(msg)
        self.notificator.send_message(msg)
        self.start_working()
