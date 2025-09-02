import argparse
import os
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import AnyData
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list
from kasperl.api import make_list, StreamWriter


class TextFileWriter(StreamWriter, InputBasedPlaceholderSupporter):

    def __init__(self, path: str = None, append: bool = None, delete_on_initialize: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param path: the file to write to
        :type path: str
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
        self.path = path
        self.append = append
        self.delete_on_initialize = delete_on_initialize

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
        return "Stores the incoming data in the specified text file."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-p", "--path", metavar="FILE", type=str, help="The file to write the data to; " + placeholder_list(obj=self), required=True)
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
        self.path = ns.path
        self.append = ns.append
        self.delete_on_initialize = ns.delete_on_initialize

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        path = self.session.expand_placeholders(self.path)
        if os.path.exists(path) and os.path.isfile(path):
            self.logger().info("Deleting during initialization: %s" % path)
            os.rmdir(path)

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def write_stream(self, data):
        """
        Saves the data one by one.

        :param data: the data to write (single record or iterable of records)
        """
        for item in make_list(data):
            path = self.session.expand_placeholders(self.path)
            if self.append:
                with open(path, "a") as fp:
                    fp.write(str(item))
                    fp.write("\n")
            else:
                with open(path, "w") as fp:
                    fp.write(str(item))
                    fp.write("\n")
