import argparse
import os
from typing import Optional, List, Dict

from wai.logging import LOGGING_WARNING
from kasperl.api import SingleVariableGenerator


class TextFileGenerator(SingleVariableGenerator):
    """
    Outputs the lines from the text file (non-empty and not starting with #).
    """

    def __init__(self, text_file: str = None, var_name: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param text_file: the text file with the values to use
        :type text_file: str
        :param var_name: the variable name
        :type var_name: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(var_name=var_name, logger_name=logger_name, logging_level=logging_level)
        self.text_file = text_file

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "text-file"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Outputs the lines from the text file (non-empty and not starting with #)."

    def _default_var_name(self) -> str:
        """
        Returns the default variable name.

        :return: the default name
        :rtype: str
        """
        return "v"

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-f", "--text_file", type=str, metavar="FILE", default=None, help="The text file with the values to use.", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.text_file = ns.text_file

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        result = super()._check()

        if result is None:
            if not os.path.exists(self.text_file):
                return "Text file does not exist: %s" % self.text_file
            if os.path.isdir(self.text_file):
                return "Text file points to a directory: %s" % self.text_file

        return result

    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        result = []
        with open(self.text_file, "r") as fp:
            values = fp.readlines()
            for value in values:
                value = value.strip()
                if (len(value) == 0) or value.startswith("#"):
                    continue
                result.append({self.var_name: str(value)})
        return result
