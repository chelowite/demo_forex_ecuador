"""
Main module of the Forex Batch DB
"""
from time import (sleep, time)
import importlib
from datetime import (datetime, timedelta)

import environment_config as config

from utils.batch import BatchBase
from utils.to_process import (DONE_ARCHIVED, ERROR, INSERTED, FORCE_PROCESS)
from utils.last_exec import LastExec

from forex.db import DB
from forex.to_parse_forex import ToParseForex
from forex.parser.errors import (ParserError, QuotaExceeded)
from forex.pair_package import generate_pair_package_with_default_pairs

# Batch
BATCH_ARGUMENTS = [
    {
        "option_string": ["instance"],
        "type": str,
        "help": "Internal process identifier for Forex Batch DB"
    }
]

# Constants
LOCKED_TO_PARSE_FOREX_CHECK_TIME = 60 * 60 # in seconds (1 hour)
LOCKED_TO_PARSE_FOREX_MAX_TIME = 60 * 60 # in seconds (1 hour)
SLEEP_TIME = 60 * 5 # in seconds (5 minutes)

# TODO: In the future this is should be removed
# There could be multiple sources that could be
# used for different pairs and currencies
CURRENT_FOREX_SOURCE = "fixer"

def postpone_date(d: datetime) -> datetime:
    return d + timedelta(days=1)

class Batch(BatchBase):
    """
    Forex Batch DB
    ---
    This batch is intended to pull data **on demand**
    using to_parse_forex objects which contains as
    payload the date to parse and also the currencies
    needed.
    """
    def __init__(self, **kwargs):
        super().__init__(__name__)

        self.good_counter = 0
        self.errors_counter = 0
        self.woke_from_nap_and_start_working = False

        try:
            self.db = DB()
        except RuntimeError as e:
            error_msg = f"ERROR. Aborting process: {e}"
            self.notificator.send_message(error_msg)
            self.logger.exception(error_msg)
            raise

        self.last_exec = LastExec(__name__, param=kwargs['instance'], server=LastExec.get_server_name())

        _ = kwargs # Not in use, but will raise error if it isn't in parameters

    def get_parser(self, source_name: str):
        """Get a parser for a specific source"""
        module_name = f"forex.parser.{source_name}"
        source_module = importlib.import_module(module_name)
        return source_module.Parser()

    def start_parsing(self, parser, current_to_parse_forex):
        """
        Begin parsing all currencies using the exchangeDate
        provided by the current_to_parse_forex
        """
        self.logger.info("Start parsing")
        if config.DEBUG_MODE is True:
            # Parse without catching errors (interrupts at the first error)
            self.logger.debug("Generating pair package with default pairs")
            pair_package = generate_pair_package_with_default_pairs(
                current_to_parse_forex.pairPackage.exchangeDate
            )
            pair_package = parser.pull(pair_package)
            self.db.update_pair_package(pair_package)
            current_to_parse_forex.recordStatus = DONE_ARCHIVED
            self.logger.info("Parse succeeded!")
            self.good_counter += 1
        else:
            # Parse catching errors (if it fails, it continues anyways)
            try:
                error_msg = None
                self.logger.debug("Generating pair package with default pairs")
                pair_package = generate_pair_package_with_default_pairs(
                    current_to_parse_forex.pairPackage.exchangeDate
                )
                pair_package = parser.pull(pair_package)
                self.db.update_pair_package(pair_package)
                current_to_parse_forex.recordStatus = DONE_ARCHIVED
                self.logger.info("Parse succeeded!")
                self.good_counter += 1
            except QuotaExceeded as e:
                msg = f"{CURRENT_FOREX_SOURCE} Quota Exceeded. ToParseForex postponed"
                self.logger.warning(msg)
                self.notificator.send_message(msg)
                current_to_parse_forex.recordStatus = INSERTED
                current_to_parse_forex.recordNotes.append(f"Quota exceeded. {e}. ToParseForex postponed")
                current_to_parse_forex.processingDate = postpone_date(
                    current_to_parse_forex.processingDate
                )
            except ParserError as e:
                error_msg = f"Parser error. Unable to complete: {e}"
            except ValueError as e:
                error_msg = f"Value error. Unable to complete: {e}"
            except TypeError as e:
                error_msg = f"Type error. Unable to complete: {e}"
            except (SystemError, RuntimeError) as e:
                error_msg = f"Fatal and UNEXPECTED error. Unable to complete. {e}"
            except Exception as e:
                error_msg = f"Exception OOOPS!. {e}"
            finally:
                if error_msg is not None:
                    current_to_parse_forex.recordStatus = ERROR
                    current_to_parse_forex.recordNotes.append(error_msg)
                    self.logger.error(error_msg)
                    self.notificator.send_message(error_msg)
                    self.errors_counter += 1
        self.logger.info("Parse finished")
        return current_to_parse_forex

    def start_working(self):
        """Start indefinite batch loop"""
        self.logger.debug("Starting indefinite loop")
        last_check_locked_to_parse_forex = None
        while True:
            if last_check_locked_to_parse_forex is None or\
                time() - last_check_locked_to_parse_forex > LOCKED_TO_PARSE_FOREX_CHECK_TIME:
                self.logger.info("Removing lock of old ToParseForex")
                self.db.remove_lock_to_parse_forex(LOCKED_TO_PARSE_FOREX_MAX_TIME)
                last_check_locked_to_parse_forex = time()

            self.logger.info("Checking for unlocked ToParseForex")
            r = self.db.get_and_lock_first_to_parse_forex()
            current_to_parse_forex = ToParseForex(**r) if r else None

            if current_to_parse_forex is not None:
                self.logger.info("One ToParseForex got")
                if self.woke_from_nap_and_start_working is True:
                    # I sum 1 which is the current ToProcess but it is LOCKED instead of WAITING
                    pending_to_parse_forex = self.db.count_waiting_to_parse_forex() + 1
                    msg = f"Resuming parsing. Encountered {pending_to_parse_forex} waiting ToParseForex."
                    self.logger.info(msg)
                    self.notificator.send_message(msg)
                    self.woke_from_nap_and_start_working = False

                current_to_parse_forex.clean_record_notes() # We want to add new notes if needed, so we delete old ones

                if current_to_parse_forex.recordStatus != FORCE_PROCESS and \
                    self.db.get_pair_package(current_to_parse_forex.pairPackage):
                    msg = "All pairs are already in the DB. Skipped parsing"
                    self.logger.info(msg)
                    current_to_parse_forex.recordNotes.append(msg)
                    current_to_parse_forex.recordStatus = DONE_ARCHIVED
                    self.good_counter += 1
                else:
                    parser = self.get_parser(CURRENT_FOREX_SOURCE)
                    current_to_parse_forex = self.start_parsing(parser, current_to_parse_forex)

                self.db.get_and_update_to_parse_forex(current_to_parse_forex)
                self.last_exec.save() # Save last execution
            else:
                self.logger.info(f"Nothing to do. I'm taking a nap... ({SLEEP_TIME}s)")
                sleep(SLEEP_TIME)
                if self.woke_from_nap_and_start_working is False:
                    self.logger.debug(f"Counters before reseting: OK {self.good_counter}/ Error {self.errors_counter}")
                    self.good_counter, self.errors_counter = 0, 0
                    self.woke_from_nap_and_start_working = True

    def main(self):
        """External call to this batch to start operating"""
        msg = f"Starting {__name__} for to_parse_forex | Debug: {config.DEBUG_MODE}"
        self.logger.info(msg)
        self.notificator.send_message(msg)
        self.start_working()
