import abc
import argparse
import os
from typing import List, Any

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list, NameSupporter, SourceSupporter
from seppl import AnyData
from seppl.io import BatchFilter

DEFAULT_ALLOWED = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-."


class SanitizeName(BatchFilter, abc.ABC):
    """
    Removes unwanted characters from file names.
    """

    def __init__(self, allowed: str = None, replace: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param allowed: the allowed characters
        :type allowed: str
        :param replace: the character to use as replacement (can be empty)
        :type replace: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.allowed = allowed
        self.replace = replace
        self._count = None
        self._sanitized = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "sanitize-name"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Removes unwanted characters from file names."

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
        parser.add_argument("-a", "--allowed", type=str, help="The characters that are allowed in names.", default=DEFAULT_ALLOWED, required=False)
        parser.add_argument("-r", "--replace", type=str, help="The string to replace the unwanted characters with, leave empty to complete replace them.", default="", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.allowed = ns.allowed
        self.replace = ns.replace

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        self._count = 0
        self._sanitized = 0
        if self.allowed is None:
            self.allowed = DEFAULT_ALLOWED
        if self.replace is None:
            self.replace = ""

    @abc.abstractmethod
    def _duplicate(self, item: Any, path: str, name_new: str) -> Any:
        """
        Duplicates the data item using the new name.

        :param item: the item to duplicate
        :param path: the path of the item
        :type path: str
        :param name_new: the new name
        :type name_new: str
        :return: the duplicated item
        """
        raise NotImplementedError()

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            # name
            if not isinstance(item, NameSupporter):
                self.logger().warning("Does not implement '%s', cannot retrieve name for check, not discarding!" % str(NameSupporter))
                result.append(item)
                continue
            name = item.get_name()

            # source
            if not isinstance(item, SourceSupporter):
                self.logger().warning("Does not implement '%s', cannot retrieve name for check, not discarding!" % str(SourceSupporter))
                result.append(item)
                continue
            if item.get_source() is None:
                path = "."
            else:
                path = os.path.dirname(item.get_source())

            # increment counter
            self._count += 1

            # check chars
            updated = False
            name_new = ""
            for c in name:
                if c not in self.allowed:
                    updated = True
                    name_new += self.replace
                else:
                    name_new += c

            if updated:
                self._sanitized += 1
                self.logger().info("Result: %s -> %s" % (name, name_new))
                item_new = self._duplicate(item, path, name_new)
                result.append(item_new)
            else:
                result.append(item)

        return flatten_list(result)
