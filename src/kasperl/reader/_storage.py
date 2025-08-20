import argparse
from typing import List, Iterable

from wai.logging import LOGGING_WARNING

from kasperl.api import Reader
from seppl import AnyData


class StorageReader(Reader):

    def __init__(self, storage_name: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param storage_name: the name of the storage item to retrieve
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
        return "from-storage"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Retrieves the specified object from internal storage."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-s", "--storage_name", type=str, help="The name of the object to retrieve from internal storage.", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.storage_name = ns.storage_name

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
        if self.storage_name is None:
            raise Exception("No storage name defined!")

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        if self.storage_name in self.session.storage:
            self.logger().info("Retrieving from storage: %s" % self.storage_name)
            yield self.session.storage[self.storage_name]
        else:
            raise Exception("Not available from storage: %s" + self.storage_name)

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return True
