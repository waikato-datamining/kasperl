import abc
import argparse
from typing import List, Dict

from wai.logging import LOGGING_WARNING

from kasperl.api import DataFormatter, StreamWriter, make_list
from seppl import AnyData, Plugin
from seppl.variables import InputBasedVariableSupporter


class ConsoleWriter(StreamWriter, InputBasedVariableSupporter, abc.ABC):

    def __init__(self, data_formatter: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param data_formatter: the data formatter to apply (plugin name + options)
        :type data_formatter: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.data_formatter = data_formatter
        self._data_formatter = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "console"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Prints the data to stdout using the supplied data formatter. Any other variables will get expanded as well."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-f", "--data_formatter", type=str, help="The data formatter to apply", required=False, default="df-simple-string")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.data_formatter = ns.data_formatter

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    @abc.abstractmethod
    def _available_data_formatters(self) -> Dict[str, Plugin]:
        """
        Returns the available data formatter plugins.

        :return: the plugins (name -> plugin)
        :rtype: dict
        """
        raise NotImplementedError()

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.data_formatter is None:
            self.data_formatter = "df-simple-string"
        if self._data_formatter is None:
            self._data_formatter = DataFormatter.parse_dataformatter(self.data_formatter, self._available_data_formatters())

    def write_stream(self, data):
        """
        Saves the data one by one.

        :param data: the data to write (single record or iterable of records)
        """
        for item in make_list(data):
            item_str = self._data_formatter.format_data(item)
            item_str = self.session.expand_variables(item_str)
            print(item_str)
