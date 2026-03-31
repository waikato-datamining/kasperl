import argparse
from typing import List

from seppl import AnyData
from seppl.io import BatchFilter
from seppl.placeholders import add_placeholder, InputBasedPlaceholderSupporter, placeholder_list
from wai.logging import LOGGING_WARNING


class LogPlaceholder(BatchFilter, InputBasedPlaceholderSupporter):

    def __init__(self, placeholders: List[str] = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param placeholders: the names of the placeholder (without curly brackets)
        :type placeholders: list
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.placeholders = placeholders

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "log-placeholder"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Outputs the values of the specified placeholders. Logging must be set to INFO for the output to show."

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
        parser.add_argument("-p", "--placeholder", type=str, help="The name of the placeholders, without curly brackets.", default=None, required=True, nargs="+")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.placeholders = ns.placeholder

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if (self.placeholders is None) or (len(self.placeholders) == 0):
            raise Exception("No placeholder names provided!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        for placeholder in self.placeholders:
            self.logger().info("%s -> %s" % (placeholder, self.session.expand_placeholders("{" + placeholder + "}")))
        return data
