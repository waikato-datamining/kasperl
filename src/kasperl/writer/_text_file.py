import abc
import argparse
import os
from typing import List, Dict

from wai.logging import LOGGING_WARNING

from seppl import AnyData, Plugin
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list
from kasperl.api import make_list, StreamWriter, DataFormatter


class TextFileWriter(StreamWriter, InputBasedPlaceholderSupporter, abc.ABC):

    def __init__(self, data_formatter: str = None, output_file: str = None, append: bool = None, delete_on_initialize: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the writer.

        :param data_formatter: the data formatter to apply (plugin name + options)
        :type data_formatter: str
        :param output_file: the file to write to
        :type output_file: str
        :param append: whether to append the file rather than overwrite
        :type append: bool
        :param delete_on_initialize: whether to delete an existing file when initializing the writer
        :type delete_on_initialize: bool
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.data_formatter = data_formatter
        self.output_file = output_file
        self.append = append
        self.delete_on_initialize = delete_on_initialize
        self._data_formatter = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "to-text-file"

    def description(self) -> str:
        """
        Returns a description of the writer.

        :return: the description
        :rtype: str
        """
        return "Applies the specified data formatter to the incoming data and stores the result in the specified text file."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-f", "--data_formatter", type=str, help="The data formatter to apply", required=False, default="df-simple-string")
        parser.add_argument("-o", "--output_file", metavar="FILE", type=str, help="The file to write the data to; " + placeholder_list(obj=self), required=True)
        parser.add_argument("-a", "--append", action="store_true", help="Whether to append the file rather than overwrite it.")
        parser.add_argument("-d", "--delete_on_initialize", action="store_true", help="Whether to remove any existing file when initializing the writer.")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.data_formatter = ns.data_formatter
        self.output_file = ns.output_file
        self.append = ns.append
        self.delete_on_initialize = ns.delete_on_initialize

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.data_formatter is None:
            self.data_formatter = "df-simple-string"
        if self._data_formatter is None:
            self._data_formatter = DataFormatter.parse_dataformatters(self.data_formatter, available_dataformatters=self._available_data_formatters())
        output_file = self.session.expand_placeholders(self.output_file)
        if os.path.exists(output_file) and os.path.isfile(output_file):
            self.logger().info("Deleting during initialization: %s" % output_file)
            os.remove(output_file)

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def _available_data_formatters(self) -> Dict[str, Plugin]:
        """
        Returns the available data formatter plugins.

        :return: the plugins (name -> plugin)
        :rtype: dict
        """
        raise NotImplementedError()

    def write_stream(self, data):
        """
        Saves the data one by one.

        :param data: the data to write (single record or iterable of records)
        """
        for item in make_list(data):
            item_str = self._data_formatter.format_data(item)
            output_file = self.session.expand_placeholders(self.output_file)
            if self.append:
                with open(output_file, "a") as fp:
                    fp.write(item_str)
                    fp.write("\n")
            else:
                with open(output_file, "w") as fp:
                    fp.write(item_str)
                    fp.write("\n")
