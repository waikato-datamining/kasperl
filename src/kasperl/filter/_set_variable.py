import argparse
from typing import List

from seppl import AnyData, AliasSupporter
from seppl.io import BatchFilter
from seppl.variables import add_variable, InputBasedVariableSupporter, variable_list
from wai.logging import LOGGING_WARNING


class SetVariable(BatchFilter, InputBasedVariableSupporter, AliasSupporter):

    def __init__(self, variable: str = None, value: str = None, use_current: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param variable: the name of the variable (without curly brackets)
        :type variable: str
        :param value: the value of the variable
        :type value: str
        :param use_current: whether to use the current data passing through instead of the value
        :type use_current: bool
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.variable = variable
        self.value = value
        self.use_current = use_current

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "set-variable"

    def aliases(self) -> List[str]:
        """
        Returns the aliases under which the plugin is known under/available as well.

        :return: the aliases
        :rtype: list
        """
        return ["set-placeholder"]

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Sets the variable to the specified value when data passes through. The value can contain other variables, which get expanded each time data passes through. Can use the data passing through instead of specified value as well."

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
        parser.add_argument("-V", "-p", "--variable", "--placeholder", dest="variable", type=str, help="The name of the variable, without curly brackets.", default=None, required=True)
        parser.add_argument("-v", "--value", type=str, help="The value of the variable, may contain other variables. " + variable_list(obj=self), default=None, required=False)
        parser.add_argument("-u", "--use_current", action="store_true", help="Whether to use the data passing through instead of the specified value.", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.variable = ns.variable
        self.value = ns.value
        self.use_current = ns.use_current

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.variable is None:
            raise Exception("No variable name provided!")
        if (not self.use_current) and (self.value is None):
            raise Exception("No variable value provided!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        if self.use_current:
            value = str(data)
        else:
            value = self.value
        value = self.session.expand_variables(value)
        self.logger().info("%s -> %s" % (self.variable, value))
        add_variable(self.variable, "no description", False, lambda i: value)
        return data
