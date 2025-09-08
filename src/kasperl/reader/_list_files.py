import argparse
import os
import re
from typing import List, Iterable

from wai.logging import LOGGING_WARNING

from kasperl.api import Reader
from seppl.placeholders import placeholder_list, PlaceholderSupporter


class ListFiles(Reader, PlaceholderSupporter):

    def __init__(self, input_dir: str = None, regexp: str = None, as_list: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param input_dir: the directory to list files from
        :type input_dir: str
        :param regexp: the regular expression that the files must match
        :type regexp: str
        :param as_list: whether to output the files as a list rather than one by one
        :type as_list: bool
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.input_dir = input_dir
        self.regexp = regexp
        self.as_list = as_list
        self._finished = False

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "list-files"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Lists files in the specified directory and forwards them."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-i", "--input_dir", metavar="DIR", type=str, help="The directory to list the files in; " + placeholder_list(obj=self), required=True)
        parser.add_argument("-r", "--regexp", metavar="REGEXP", type=str, help="The regular expression that the files must match.", required=False, default=".*")
        parser.add_argument("--as_list", action="store_true", help="Whether to forward the files as a list or one by one.", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.input_dir = ns.input_dir
        self.regexp = ns.regexp
        self.as_list = ns.as_list

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        if self.as_list:
            return [List[str]]
        else:
            return [str]

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.input_dir is None:
            raise Exception("No input_dir specified!")
        if self.regexp is None:
            self.regexp = ".*"
        if self.as_list is None:
            self.as_list = False

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        input_dir = self.session.expand_placeholders(self.input_dir)
        if not os.path.exists(input_dir):
            raise Exception("input_dir does not exist: %s" % input_dir)
        if not os.path.isdir(input_dir):
            raise Exception("Not a directory: %s" % input_dir)
        self.logger().info("Listing files in: %s" % input_dir)
        self._finished = False
        files = []
        for f in os.listdir(input_dir):
            full = os.path.join(input_dir, f)
            if os.path.isdir(full):
                continue
            if re.match(self.regexp, f):
                files.append(full)
        if self.as_list:
            yield files
        else:
            for f in files:
                yield f
        self._finished = True

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return self._finished
