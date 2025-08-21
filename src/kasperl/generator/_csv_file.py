import argparse
import csv
import os
import re
import traceback
from typing import Optional, List, Dict, Tuple

from wai.logging import LOGGING_WARNING
from kasperl.api import Generator


class CSVFileGenerator(Generator):
    """
    Forwards the values in the columns of the CSV file, using the column headers as variable names.
    """

    def __init__(self, csv_file: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param csv_file: the path to search for directories
        :type csv_file: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.csv_file = csv_file

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "csv-file"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Forwards the values in the columns of the CSV file, using the column headers as variable names."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-f", "--csv_file", type=str, metavar="FILE", help="The CSV file to use.", required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.csv_file = ns.csv_file

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        result = super()._check()

        if result is None:
            if not os.path.exists(self.csv_file):
                return "CSV file does not exist: %s" % self.csv_file
            if os.path.isdir(self.csv_file):
                return "CSV file points to a directory: %s" % self.csv_file

        return result

    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        result = []

        with open(self.csv_file, "r") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                result.append(row)

        return result
