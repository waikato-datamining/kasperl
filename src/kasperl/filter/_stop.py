import abc
import argparse
from typing import List, Dict

from wai.logging import LOGGING_WARNING

from seppl import AnyData, MetaDataHandler, Plugin
from seppl.io import BatchFilter
from kasperl.api import make_list, flatten_list, compare_values, \
    COMPARISONS_EXT, COMPARISON_EQUAL, COMPARISON_CONTAINS, COMPARISON_MATCHES, COMPARISON_EXT_HELP, DataFormatter


class Stop(BatchFilter):
    """
    Raises an exception when the expression evaluates to True, stopping the pipeline.
    """

    def __init__(self, field: str = None, data_formatter: str = None, comparison: str = COMPARISON_EQUAL, value=None,
                 stop_message: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param field: the name of the meta-data field to perform the comparison on
        :type field: str
        :param data_formatter: the data formatter to apply to the data instead of using the meta-data field (plugin name + options)
        :type data_formatter: str
        :param comparison: the comparison to perform
        :type comparison: str
        :param value: the value to compare with
        :param stop_message: the message to use in the exception
        :type stop_message: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.field = field
        self.data_formatter = data_formatter
        self.comparison = comparison
        self.value = value
        self.stop_message = stop_message
        self._data_formatter = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "stop"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Raises an exception when the expression evaluates to True, stopping the pipeline. " \
               "Never raises an exception if no meta-data field/data-formatter specified. " \
               "Performs the following comparison: METADATA_VALUE/DATA_VALUE COMPARISON VALUE."

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
        parser.add_argument("--data_formatter", type=str, metavar="CMDLINE", help="The data formatter to apply to the data passing through for obtaining the value instead of the one from the meta-data (plugin name and options).", required=False, default=None)
        parser.add_argument("--comparison", choices=COMPARISONS_EXT, default=COMPARISON_EQUAL, help="How to compare the value with the meta-data/data-formatter value; " + COMPARISON_EXT_HELP
                            + "; in case of '" + COMPARISON_CONTAINS + "' and '" + COMPARISON_MATCHES + "' the supplied value represents the substring to find/regexp to search with", required=False)
        parser.add_argument("--value", type=str, help="The user-supplied value to use in the comparison", default=None, required=False)
        parser.add_argument("--stop_message", type=str, help="The message to use in the exception.", default=None, required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.field = ns.field
        self.data_formatter = ns.data_formatter
        self.comparison = ns.comparison
        self.value = ns.value
        self.stop_message = ns.stop_message

    @abc.abstractmethod
    def _available_data_formatters(self) -> Dict[str, Plugin]:
        """
        Returns the available data formatter plugins.

        :return: the plugins (name -> plugin)
        :rtype: dict
        """
        raise NotImplementedError()

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()

        if (self.field is not None) and (self.data_formatter is None):
            raise Exception("Meta-data field and data-formatter cannot be specified at the same time!")
        if ((self.field is not None) or (self.data_formatter is not None)) and (self.value is None):
            raise Exception("No value provided to compare with!")
        if (self.field is None) and (self.data_formatter is None) and (self.value is not None):
            raise Exception("Value provided but neither meta-data field nor data-formatter!")
        if (self.data_formatter is not None) and (self._data_formatter is None):
            self._data_formatter = DataFormatter.parse_dataformatter(self.data_formatter, self._available_data_formatters())

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            # evaluate expression?
            v1 = None
            v2 = self.value
            if self.field is not None:
                meta = None
                if isinstance(item, MetaDataHandler):
                    if item.has_metadata():
                        meta = item.get_metadata()
                if meta is not None:
                    v1 = meta[self.field]
            elif self._data_formatter is not None:
                v1 = self._data_formatter.format_data(item)

            if v1 is not None:
                comp_result = compare_values(v1, self.comparison, v2)
                comp = v1 + " " + self.comparison + " " + v2 + " = " + str(comp_result)
                if self.field is not None:
                    msg = "Field '%s': '%s'" % (self.field, comp)
                else:
                    msg = "Data formatter: '%s'" % comp
                self.logger().info(msg)
                if comp_result:
                    self.session.stopped = True
                    if self.stop_message is None:
                        self.logger().error(msg)
                    else:
                        self.logger().error(self.stop_message)

            result.append(item)

        return flatten_list(result)
