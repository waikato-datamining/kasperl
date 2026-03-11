import argparse
import os
import re
import traceback
from typing import Optional, List, Dict, Tuple

from wai.logging import LOGGING_WARNING
from kasperl.api import Generator


VAR_ABSFILE = "absfile"
VAR_RELFILE = "relfile"
VAR_FILENAME = "filename"
VAR_FILENAME_NOEXT = "filename_noext"
FILES_VARS = [
    VAR_ABSFILE,
    VAR_RELFILE,
    VAR_FILENAME,
    VAR_FILENAME_NOEXT,
]


class FileGenerator(Generator):
    """
    Iterates over files that it finds.
    """

    def __init__(self, path: str = None, recursive: bool = False, regexp: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param path: the path to search for directories
        :type path: str
        :param recursive: whether to search recursively
        :type recursive: bool
        :param regexp: the regular expression for matching directories
        :type regexp: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.path = path
        self.recursive = recursive
        self.regexp = regexp

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "files"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Iterates over files that it finds. Can be limited to files that match a regular expression. " \
            + "Available variables: " + "|".join(FILES_VARS) + ". " \
            + VAR_ABSFILE + ": the absolute file, " \
            + VAR_RELFILE + ": the relative file to the search path, " \
            + VAR_FILENAME + ": the file name (no parent path), " \
            + VAR_FILENAME_NOEXT + ": the file name without extension (no parent path)."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-p", "--path", type=str, metavar="DIR", default=None, help="The directory/directories to search", required=True, nargs="+")
        parser.add_argument("-r", "--recursive", action="store_true", help="Whether to search for files recursively.", required=False)
        parser.add_argument("--regexp", type=str, metavar="REGEXP", default=None, help="The regular expression to use for matching files; matches all if not provided.", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.path = ns.path
        self.recursive = ns.recursive
        self.regexp = ns.regexp

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        result = super()._check()

        if result is None:
            if self.regexp == "":
                self.regexp = None
            if self.regexp is not None:
                try:
                    re.compile(self.regexp)
                except:
                    result = "Invalid regular expression: %s\n%s" % (self.regexp, traceback.format_exc())

        if result is None:
            for p in self.path:
                if not os.path.exists(p):
                    result = "Directory does not exist: %s" % p
                elif not os.path.isdir(p):
                    result = "Not a directory: %s" % p

        return result

    def _locate(self, start: str, current: str, recursive: bool, paths: List[Tuple[str, str]]):
        """
        Locates directories.

        :param start: the starting directory (for determining relative dirs)
        :type start: str
        :param current: the directory to search
        :type current: str
        :param recursive: whether to search recursively
        :type recursive: bool
        :param paths: for collecting the matching dirs
        :type paths: list
        """
        for f in os.listdir(current):
            full = os.path.join(current, f)
            if os.path.isfile(full):
                if self.regexp is not None:
                    m = re.match(self.regexp, f)
                    if m is not None:
                        paths.append((start, full))
                else:
                    paths.append((start, full))
            if os.path.isdir(full) and recursive:
                self._locate(start, full, recursive, paths)

    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        result = []

        # locate files
        paths = []
        for abs_file in self.path:
            self._locate(os.path.abspath(abs_file), os.path.abspath(abs_file), self.recursive, paths)

        # prepare variables
        for parent_dir, abs_file in paths:
            if abs_file.startswith(parent_dir):
                rel_file = abs_file[len(parent_dir):]
                if rel_file.startswith("/") or rel_file.startswith("\\"):
                    rel_file = rel_file[1:]
            else:
                rel_file = None
            filename = os.path.basename(abs_file)
            filename_noext = os.path.split(filename)[0]
            result.append({
                VAR_ABSFILE: abs_file,
                VAR_RELFILE: rel_file,
                VAR_FILENAME: filename,
                VAR_FILENAME_NOEXT: filename_noext,
            })

        return result
