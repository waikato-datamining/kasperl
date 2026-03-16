import argparse
from typing import List

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list
from seppl import MetaDataHandler, AnyData
from seppl.io import BatchFilter


class GetMetaData(BatchFilter):
    """
    Returns the value assoicated with the specified key from the meta-data.
    """

    def __init__(self, field: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param field: the name of the meta-data field to retrieve
        :type field: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)

        self.field = field

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "get-metadata"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Returns the value of the specified key from the meta-data."

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
        parser.add_argument("-f", "--field", type=str, help="The meta-data field to set", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.field = ns.field

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.field is None:
            raise Exception("No meta-data field provided!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []

        for item in make_list(data):
            meta = None
            if isinstance(item, MetaDataHandler):
                if item.has_metadata():
                    meta = item.get_metadata()

            if meta is None:
                self.logger().warning("No meta-data, ignoring")
            else:
                if self.field in meta:
                    value = meta[self.field]
                    self.logger().info("Getting meta-data: %s=%s" % (self.field, str(value)))
                    result.append(value)
                else:
                    self.logger().warning("Field not present: %s" % self.field)

        return flatten_list(result)
