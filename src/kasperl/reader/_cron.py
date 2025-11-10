import argparse

from time import sleep
from typing import List, Iterable

from wai.logging import LOGGING_WARNING
from seppl import AnyData
from kasperl.api import Reader
from croniter import croniter
from datetime import datetime


class Cron(Reader):
    """
    Dummy reader that forwards a string whenever the timestamp defined by the cron-expression is reached.
    """

    def __init__(self, cron_expr: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the handler.

        :param cron_expr: the cron expression to use
        :type cron_expr: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.cron_expr = cron_expr
        self._iter = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "cron"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Dummy reader that forwards a string whenever the timestamp defined by the cron-expression is reached. " \
               "For more information on Cron, see: https://en.wikipedia.org/wiki/Cron"

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-e", "--cron_expr", metavar="EXPR", type=str, help="The cron expression to use: [sec] min hour day month day_of_week", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.cron_expr = ns.cron_expr

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        self._iter = croniter(self.cron_expr, datetime.now().astimezone(), second_at_beginning=True)

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        while not self.session.stopped:
            next_timestamp = self._iter.get_next(datetime)
            self.logger().info("Next execution: %s" % str(next_timestamp))
            while (datetime.now().astimezone() < next_timestamp) and not (self.session.stopped):
                sleep(0.1)
            yield str(next_timestamp)

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return False
