import argparse
import os
import shutil
from typing import List, Optional

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list
from seppl.io import BatchFilter
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list


class MoveFiles(BatchFilter, InputBasedPlaceholderSupporter):
    """
    Moves the files it receives into the specified target directory and forwards the new files.
    """

    def __init__(self, target_dir: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param target_dir: the directory to move the files to
        :type target_dir: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.target_dir = target_dir

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "move-files"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Moves the files it receives into the specified target directory and forwards the new files."

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [str, list]

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        return [str, list]

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-t", "--target_dir", metavar="DIR", type=str, help="The directory to move the files to. " + placeholder_list(obj=self), required=True)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.target_dir = ns.target_dir

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.target_dir is None:
            raise Exception("Target directory not specified!")

    def _move(self, source: str, target_path: str) -> Optional[str]:
        """
        Moves the file to the target directory and returns the new path.
        Returns None in case it encounters directories.

        :param source: the path to move
        :type source: str
        :param target_path: the directory to move to
        :type target_path: str
        :return: the new path, None if input was a directory (gets skipped) or failed to move
        :rtype: str or None
        """
        if not os.path.isfile(source):
            return None

        dest = os.path.join(target_path, os.path.basename(source))
        try:
            self.logger().info("Moving '%s' to '%s'..." % (source, dest))
            shutil.move(source, dest)
            return dest
        except:
            self.logger().error("Failed to move '%s' to '%s'!" % (source, dest))
        return None

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        # apply placeholders
        target_path = self.session.expand_placeholders(self.target_dir)
        if target_path != self.target_dir:
            self.logger().info("Expanded target dir: %s" % target_path)
        if not os.path.exists(target_path):
            raise Exception("Target directory does not exist: %s" % target_path)
        if not os.path.isdir(target_path):
            raise Exception("Target directory is not a directory: %s" % target_path)

        # move files
        result = []
        for item in make_list(data):
            if isinstance(item, list):
                for subitem in item:
                    new_path = self._move(subitem, target_path)
                    if new_path is not None:
                        result.append(new_path)
            else:
                new_path = self._move(item, target_path)
                if new_path is not None:
                    result.append(new_path)

        return flatten_list(result)
