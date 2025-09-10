import argparse
from typing import List, Any

from wai.logging import LOGGING_WARNING

from seppl import MetaDataHandler, AnyData, METADATA_TYPES, METADATA_TYPE_STRING, METADATA_TYPE_BOOL, METADATA_TYPE_NUMERIC
from seppl.io import BatchFilter
from kasperl.api import make_list, flatten_list


class SetMetaData(BatchFilter):
    """
    Sets the specified key/value pair in the meta-data.
    """

    def __init__(self, field: str = None, value=None, as_type: str = None, use_current: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param field: the name of the meta-data field to set
        :type field: str
        :param value: the value to set
        :param as_type: the type to convert the value to
        :type as_type: str
        :param use_current: whether to use the current data passing through instead of the value
        :type use_current: bool
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)

        self.field = field
        self.value = value
        self.as_type = as_type
        self.use_current = use_current
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
        return "Sets the specified key/value pair in the meta-data. Can use the data passing through instead of the specified value as well."

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
        parser.add_argument("-v", "--value", type=str, help="The value to use in the comparison", required=False)
        parser.add_argument("-t", "--as_type", choices=METADATA_TYPES, default=METADATA_TYPE_STRING, help="How to interpret the value")
        parser.add_argument("-u", "--use_current", action="store_true", help="Whether to use the data passing through instead of the specified value.", required=False)
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
        self.use_current = ns.use_current

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.field is None:
            raise Exception("No meta-data field provided!")
        if (not self.use_current) and (self.value is None):
            raise Exception("No value provided to compare with!")
        if self.as_type is None:
            self.as_type = METADATA_TYPE_STRING
        if not self.use_current:
            self._value = self._to_type(self.value)

    def _to_type(self, value: str) -> Any:
        """
        Turns the value into the specified type.

        :param value: the value to process
        :type value: str
        :return: the converted value
        """
        if self.as_type == METADATA_TYPE_STRING:
            return str(value)
        elif self.as_type == METADATA_TYPE_BOOL:
            return str(value).lower() == "true"
        elif self.as_type == METADATA_TYPE_NUMERIC:
            return float(value)
        else:
            raise Exception("Unhandled meta-data type: %s" % self.as_type)

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []

        if self.use_current:
            value = self._to_type(data)
        else:
            value = self._value

        for item in make_list(data):
            result.append(item)
            meta = None
            if isinstance(item, MetaDataHandler):
                if item.has_metadata():
                    meta = item.get_metadata()

            if meta is None:
                self.logger().info("No meta-data, ignoring")
            else:
                self.logger().info("Setting meta-data: %s=%s" % (self.field, str(value)))
                meta[self.field] = value

        return flatten_list(result)
