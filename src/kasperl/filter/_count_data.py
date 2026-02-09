import argparse
import os
from datetime import datetime
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import AnyData, MetaDataHandler
from seppl.io import BatchFilter
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list
from kasperl.api import make_list, flatten_list, SourceSupporter, NameSupporter, AnnotationHandler


PH_TIME = "{TIME}"
PH_DATE = "{DATE}"
PH_TIMESTAMP = "{TS}"
PH_NAME = "{NAME}"
PH_SOURCE = "{SOURCE}"
PH_HAS_ANNOTATION = "{HAS_ANNOTATION}"
PH_ANNOTATION = "{ANNOTATION}"
PH_META = "{META.<key>}"
PH_META_START = "{META."

FORMAT_DATE = "%Y-%m-%d"
FORMAT_TIME = "%H:%M:%S.%f"
FORMAT_TS = "%Y-%m-%d %H:%M:%S.%f"


def prefix_help() -> str:
    """
    Returns a help string for the arg parser.

    :return: the help string
    :rtype: str
    """
    return PH_DATE + ": for the current data (YYYY-MM-DD), " \
        + PH_TIME + ": for the current time (HH:MM:SS.SSSSSS), " \
        + PH_TIMESTAMP + ": for the current date/time (YYYY-MM-DD HH:MM:SS.SSSSSS), " \
        + PH_NAME + ": for NameSupporter data, " \
        + PH_SOURCE + ": for SourceSupporter data, " \
        + PH_HAS_ANNOTATION + "/" + PH_ANNOTATION + ": for AnnotationHandler data, " \
        + PH_META + ": for MetaDataHandler data (<key> is the key in the meta-data); " \
        + "use \\t for tab and \\n for new-line"


class CountData(BatchFilter):
    """
    Logs information about the data passing through, either storing it in the specified file or outputting it on stdout.
    """

    def __init__(self, prefix: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param prefix: the prefix to use for the output of the total count
        :type prefix: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.prefix = prefix
        self._count = 0

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "count-data"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Counts the items of data passing through and outputs the total at the end."

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
        parser.add_argument("-p", "--prefix", metavar="STR", type=str, help="The prefix string to use for the output of the total count.", default="total count: ")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.prefix = ns.prefix

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.prefix is None:
            self.prefix = ""
        self._count = 0

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        self._count += len(make_list(data))
        return data

    def finalize(self):
        """
        Finishes the processing, e.g., for closing files or databases.
        """
        super().finalize()
        print("%s%d" % (self.prefix, self._count))
