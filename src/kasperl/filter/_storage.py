import argparse
from typing import List

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list
from seppl import AnyData
from seppl.io import Filter


class StorageUpdater(Filter):
    """
    Renames files using a user-supplied format.
    """

    def __init__(self, storage_name: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param storage_name: the name to store the objects under in internal storage
        :type storage_name: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.storage_name = storage_name

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "set-storage"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Stores the objects passing through in internal storage using the specified name."

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
        parser.add_argument("-s", "--storage_name", type=str, help="The name to store the object under in internal storage.", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.storage_name = ns.storage_name

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            self.logger().info("Storing in storage: %s" % self.storage_name)
            self.session.storage[self.storage_name] = item
            result.append(item)
        return flatten_list(result)
