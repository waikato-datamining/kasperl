import argparse
from time import sleep
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import AnyData
from seppl.io import BatchFilter


class Sleep(BatchFilter):
    """
    Waits the specified number of seconds before forwarding the data.
    """

    def __init__(self, wait_time: float = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param wait_time: the time in seconds to wait
        :type wait_time: float
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.wait_time = wait_time

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "sleep"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Waits the specified number of seconds before forwarding the data. A time of 0 means no waiting."

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-w", "--wait_time", type=float, metavar="SEC", help="The time in seconds to wait.", default=0.0, required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.wait_time = ns.wait_time

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.wait_time is None:
            self.wait_time = 0.0

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        if self.wait_time > 0:
            self.logger().info("Waiting for %f seconds" % self.wait_time)
            sleep(self.wait_time)
        else:
            self.logger().info("Skip waiting")

        return data
