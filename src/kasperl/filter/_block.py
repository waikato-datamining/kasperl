import argparse
from typing import List

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list, compare_values, \
    COMPARISONS_EXT, COMPARISON_EQUAL, COMPARISON_CONTAINS, COMPARISON_MATCHES, COMPARISON_EXT_HELP
from seppl import AnyData, MetaDataHandler
from seppl.io import Filter


class Block(Filter):
    """
    Blocks data coming through when the expression evaluates to True.
    """

    def __init__(self, field: str = None, comparison: str = COMPARISON_EQUAL, value=None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param field: the name of the meta-data field to perform the comparison on
        :type field: str
        :param comparison: the comparison to perform
        :type comparison: str
        :param value: the value to compare with
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.field = field
        self.value = value
        self.comparison = comparison
        self._filter = None
        self._writer = None
        self._data_buffer = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "block"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Blocks data passing through if the expression evaluates to True. " \
               "Lets everything pass if no meta-data field specified. " \
               "Performs the following comparison: METADATA_VALUE COMPARISON VALUE."

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
        parser.add_argument("--field", type=str, help="The meta-data field to use in the comparison", default=None, required=False)
        parser.add_argument("--comparison", choices=COMPARISONS_EXT, default=COMPARISON_EQUAL, help="How to compare the value with the meta-data value; " + COMPARISON_EXT_HELP
                            + "; in case of '" + COMPARISON_CONTAINS + "' and '" + COMPARISON_MATCHES + "' the supplied value represents the substring to find/regexp to search with", required=False)
        parser.add_argument("--value", type=str, help="The value to use in the comparison", default=None, required=False)
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
        self.comparison = ns.comparison

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if (self.field is not None) and (self.value is None):
            raise Exception("No value provided to compare with!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            # evaluate expression?
            meta = None
            if self.field is not None:
                if isinstance(item, MetaDataHandler):
                    if item.has_metadata():
                        meta = item.get_metadata()
            if meta is not None:
                v1 = meta[self.field]
                v2 = self.value
                comp_result = compare_values(v1, self.comparison, v2)
                comp = str(meta[self.field]) + " " + self.comparison + " " + str(self.value) + " = " + str(comp_result)
                self.logger().info("Field '%s': '%s'" % (self.field, comp))
                if not comp_result:
                    continue

            result.append(item)

        return flatten_list(result)
