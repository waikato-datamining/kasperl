import argparse
import os
from typing import List, Iterable

from wai.logging import LOGGING_WARNING

from kasperl.api import Reader
from seppl.placeholders import placeholder_list, PlaceholderSupporter


class TextFileReader(Reader, PlaceholderSupporter):

    def __init__(self, path: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param path: the path of the text file to load
        :type path: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.path = path
        self._finished = False

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "from-text-file"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Reads the specified text file line by line and forwards the data."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-p", "--path", metavar="FILE", type=str, help="The file to load; " + placeholder_list(obj=self), required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.path = ns.path

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        return [str]

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.path is None:
            raise Exception("No path specified!")

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        path = self.session.expand_placeholders(self.path)
        if not os.path.exists(path):
            raise Exception("Path does not exist: %s" % path)
        if not os.path.isfile(path):
            raise Exception("Not a file: %s" % path)
        self.logger().info("Reading from: %s" % path)
        self._finished = False
        with open(path, "r") as fp:
            for line in fp.readlines():
                yield line.strip()
        self._finished = True

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return self._finished
