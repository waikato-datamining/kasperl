import abc
import argparse
import csv
import json
import os
from typing import List, Optional, Any, Dict

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list
from seppl import MetaDataHandler, AnyData
from seppl.io import BatchFilter

METADATA_FORMAT_CSV = "csv"
METADATA_FORMAT_JSON = "json"
METADATA_FORMATS = [
    METADATA_FORMAT_CSV,
    METADATA_FORMAT_JSON,
]
METADATA_FORMAT_EXTENSIONS = {
    METADATA_FORMAT_CSV: ".csv",
    METADATA_FORMAT_JSON: ".json",
}


class AttachMetaData(BatchFilter, abc.ABC):
    """
    Attaches meta-data to the data passing through. Loads the data from the specified directory based on the data's name.
    """

    def __init__(self, metadata_dir: str = None, metadata_format: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param metadata_dir: the directory with the meta-data files to load/attach
        :type metadata_dir: str
        :param metadata_format: the format that the meta-data is in
        :type metadata_format: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.metadata_dir = metadata_dir
        self.metadata_format = metadata_format
        self.attached = 0
        self.missed = 0

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "attach-metadata"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Attaches meta-data to the data passing through. " \
            + "Loads the data from the specified directory based on the data's name. " \
            + "In case of CSV, a header row is expected and the first column contains the keys and the second the values."

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
        parser.add_argument("-m", "--metadata_dir", type=str, help="The directory with the meta-data files to load/attach.", required=True)
        parser.add_argument("-f", "--metadata_format", choices=METADATA_FORMATS, default=METADATA_FORMAT_JSON, help="The format of the meta-data.", required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.metadata_dir = ns.metadata_dir
        self.metadata_format = ns.metadata_format

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.metadata_dir is None:
            raise Exception("No meta-data directory provided!")
        if self.metadata_format is None:
            self.metadata_format = METADATA_FORMAT_JSON
        if self.metadata_format not in METADATA_FORMATS:
            raise Exception("Unknown meta-data format: %s" % self.metadata_format)

    @abc.abstractmethod
    def _get_name(self, item) -> Optional[str]:
        """
        Returns the name of the item.

        :param item: the item to get the name for
        :return: the name or None if not available
        :rtype: str or None
        """
        raise NotImplementedError()

    def _load_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Loads the meta-data if available.

        :param name: the name to use for locating the meta-data
        :type name: str
        :return: the meta-data, None if not found
        :rtype: dict or None
        """
        name = os.path.splitext(name)[0] + METADATA_FORMAT_EXTENSIONS[self.metadata_format]
        path = os.path.join(self.session.expand_placeholders(self.metadata_dir), name)
        if not os.path.exists(path):
            return None

        if self.metadata_format == METADATA_FORMAT_CSV:
            result = dict()
            with open(path, newline='') as fp:
                reader = csv.reader(fp)
                first = True
                for row in reader:
                    # ignore header
                    if first:
                        first = False
                        continue
                    result[str(row[0])] = row[1]
            return result

        elif self.metadata_format == METADATA_FORMAT_JSON:
            with open(path, "r") as fp:
                return dict(json.load(fp))

        else:
            raise Exception("Unhandled meta-data format: %s" % self.metadata_format)

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []

        for item in make_list(data):
            # can we attach meta-data?
            if not isinstance(item, MetaDataHandler):
                self.logger().warning("Data does not manage meta-data, cannot attach meta-data: %s" % str(item))
                result.append(item)
                self.missed += 1
                continue

            # do we have a name?
            name = self._get_name(item)
            if name is None:
                self.logger().warning("Failed to determine name from data, cannot attach meta-data: %s" % str(item))
                result.append(item)
                self.missed += 1
                continue

            meta = self._load_metadata(name)
            if meta is None:
                self.logger().warning("No meta-data located for: %s" % name)
                self.missed += 1
            else:
                if item.has_metadata():
                    item.get_metadata().update(meta)
                else:
                    item.set_metadata(meta)
                self.attached += 1
            result.append(item)

        return flatten_list(result)

    def finalize(self):
        """
        Finishes the processing, e.g., for closing files or databases.
        """
        super().finalize()
        self.logger().info("# attached: %d" % self.attached)
        self.logger().info("# missed: %d" % self.missed)
