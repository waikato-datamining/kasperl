import argparse
from typing import Optional, List, Dict, Union

from wai.logging import LOGGING_WARNING
from kasperl.api import Generator


class PromptGenerator(Generator):
    """
    Prompts the user to enter values for the specified variables.
    """

    def __init__(self, var_names: Union[str, List[str]] = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param var_names: the variable name(s) to prompt the user with
        :type var_names: str or list
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.var_names = var_names

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "prompt"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Prompts the user to enter values for the specified variables."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-v", "--var_names", type=str, metavar="NAME", default=None, help="The list of variable names to prompt the user with.", nargs="+")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.var_names = ns.var_names

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        result = super()._check()

        if result is None:
            if (self.var_names is None) or (len(self.var_names) == 0):
                result = "No variable names specified!"

        return result

    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        result = []
        variables = dict()
        for var_name in self.var_names:
            value = input("Please enter value for variable '%s': " % var_name)
            variables[var_name] = value
        result.append(variables)
        return result
