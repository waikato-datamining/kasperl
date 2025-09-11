import argparse
from typing import Optional, List, Dict, Union

from wai.logging import LOGGING_WARNING
from kasperl.api import Generator


DEFAULT_MESSAGE = "Please enter value for variable '%s': "


class PromptGenerator(Generator):
    """
    Prompts the user to enter values for the specified variables.
    """

    def __init__(self, var_names: Union[str, List[str]] = None, message: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param var_names: the variable name(s) to prompt the user with
        :type var_names: str or list
        :param message: the prompt message to use, expects one %s in the string for the variable name
        :type message: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.var_names = var_names
        self.message = message

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
        parser.add_argument("-m", "--message", type=str, metavar="PROMPT", default=None, help="The custom message to prompt the user with; expects one %%s in the template which will get expanded with the variable name.", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.var_names = ns.var_names
        self.message = ns.message

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

        if result is None:
            if (self.message is not None) and ("%s" not in self.message):
                result = "Prompt message requires a single %s to be present!"

        return result

    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        # prompt template
        msg = DEFAULT_MESSAGE
        if self.message is not None:
            msg = self.message

        # prompt user
        result = []
        variables = dict()
        for var_name in self.var_names:
            value = input(msg % var_name)
            variables[var_name] = value
        result.append(variables)

        return result
