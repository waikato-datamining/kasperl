import argparse
from typing import List

from seppl import AnyData, AliasSupporter
from seppl.io import BatchFilter
from seppl.variables import add_variable, InputBasedVariableSupporter
from wai.logging import LOGGING_WARNING


class MetaDataToVariable(BatchFilter, InputBasedVariableSupporter, AliasSupporter):

    def __init__(self, metadata_key: str = None, variable: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param metadata_key: the meta-data key to get the variable value from
        :type metadata_key: str
        :param variable: the name of the variable (without curly brackets)
        :type variable: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.metadata_key = metadata_key
        self.variable = variable

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "metadata-to-variable"

    def aliases(self) -> List[str]:
        """
        Returns the aliases under which the plugin is known under/available as well.

        :return: the aliases
        :rtype: list
        """
        return ["metadata-to-placeholder"]

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Sets the variable with the value from the meta-data passing through."

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
        parser.add_argument("-k", "--metadata_key", type=str, help="The key in the meta-data to get the value for the variable from.", default=None, required=False)
        parser.add_argument("-V", "-p", "--variable", "--placeholder", dest="variable", type=str, help="The name of the variable, without curly brackets.", default=None, required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.variable = ns.variable
        self.metadata_key = ns.metadata_key

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.metadata_key is None:
            raise Exception("No meta-data key provided!")
        if self.variable is None:
            raise Exception("No variable name provided!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        meta = data.get_metadata()
        # no meta-data?
        if meta is None:
            self.logger().warning("No meta-data present!")
            return data
        # key not present?
        if self.metadata_key not in meta:
            self.logger().warning("Meta-data key '%s' not present!" % self.metadata_key)
            return data

        value = meta[self.metadata_key]
        self.logger().info("%s -> %s" % (self.variable, value))
        add_variable(self.variable, "from meta-data", False, lambda i: value)
        return data
