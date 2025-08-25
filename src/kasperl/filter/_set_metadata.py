import argparse
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import MetaDataHandler, AnyData, METADATA_TYPES, METADATA_TYPE_STRING, METADATA_TYPE_BOOL, METADATA_TYPE_NUMERIC
from seppl.io import Filter
from kasperl.api import make_list, flatten_list


class SetMetaData(Filter):
    """
    Sets the specified key/value pair in the meta-data.
    """

    def __init__(self, field: str = None, value=None, as_type: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param field: the name of the meta-data field to set
        :type field: str
        :param value: the value to set
        :param as_type: the type to convert the value to
        :type as_type: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)

        self.field = field
        self.value = value
        self.as_type = as_type
        self._value = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "set-metadata"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Sets the specified key/value pair in the meta-data."

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
        parser.add_argument("-f", "--field", type=str, help="The meta-data field to use in the comparison", required=True)
        parser.add_argument("-v", "--value", type=str, help="The value to use in the comparison", required=True)
        parser.add_argument("-t", "--as_type", choices=METADATA_TYPES, default=METADATA_TYPE_STRING, help="How to interpret the value")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.field = ns.field
        self.value = ns.value
        self.as_type = ns.as_type

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.field is None:
            raise Exception("No meta-data field provided!")
        if self.value is None:
            raise Exception("No value provided to compare with!")
        if self.as_type is None:
            self.as_type = METADATA_TYPE_STRING

        if self.as_type == METADATA_TYPE_STRING:
            self._value = str(self.value)
        elif self.as_type == METADATA_TYPE_BOOL:
            self._value = str(self.value).lower() == "true"
        elif self.as_type == METADATA_TYPE_NUMERIC:
            self._value = float(self.value)
        else:
            raise Exception("Unhandled meta-data type: %s" % self.as_type)

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []

        for item in make_list(data):
            result.append(item)
            meta = None
            if isinstance(item, MetaDataHandler):
                if item.has_metadata():
                    meta = item.get_metadata()

            if meta is None:
                self.logger().info("No meta-data, ignoring")
            else:
                self.logger().info("Setting meta-data: %s=%s" % (self.field, str(self.value)))
                meta[self.field] = self._value

        return flatten_list(result)
