import argparse
import os
from datetime import datetime
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import AnyData, MetaDataHandler
from seppl.io import BatchFilter
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list
from kasperl.api import make_list, flatten_list, SourceSupporter, NameSupporter, AnnotationHandler


PH_TIME = "{TIME}"
PH_DATE = "{DATE}"
PH_TIMESTAMP = "{TS}"
PH_NAME = "{NAME}"
PH_SOURCE = "{SOURCE}"
PH_HAS_ANNOTATION = "{HAS_ANNOTATION}"
PH_ANNOTATION = "{ANNOTATION}"
PH_META = "{META.<key>}"
PH_META_START = "{META."

FORMAT_DATE = "%Y-%m-%d"
FORMAT_TIME = "%H:%M:%S.%f"
FORMAT_TS = "%Y-%m-%d %H:%M:%S.%f"


def log_format_help() -> str:
    """
    Returns a help string for the arg parser.

    :return: the help string
    :rtype: str
    """
    return PH_DATE + ": for the current data (YYYY-MM-DD), " \
        + PH_TIME + ": for the current time (HH:MM:SS.SSSSSS), " \
        + PH_TIMESTAMP + ": for the current date/time (YYYY-MM-DD HH:MM:SS.SSSSSS), " \
        + PH_NAME + ": for NameSupporter data, " \
        + PH_SOURCE + ": for SourceSupporter data, " \
        + PH_HAS_ANNOTATION + "/" + PH_ANNOTATION + ": for AnnotationHandler data, " \
        + PH_META + ": for MetaDataHandler data (<key> is the key in the meta-data); " \
        + "use \\t for tab and \\n for new-line"


class LogData(BatchFilter, InputBasedPlaceholderSupporter):
    """
    Logs information about the data passing through, either storing it in the specified file or outputting it on stdout.
    """

    def __init__(self, log_format: str = None, output_file: str = None, delete_on_initialize: bool = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param log_format: the format to use in the logging output
        :type log_format: str
        :param output_file: the file to write to, uses stdout if None
        :type output_file: str
        :param delete_on_initialize: whether to delete an existing file when initializing the filter
        :type delete_on_initialize: bool
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.log_format = log_format
        self.delete_on_initialize = delete_on_initialize
        self.output_file = output_file

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "log-data"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Logs information about the data passing through, either storing it in the specified file or outputting it on stdout."

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
        parser.add_argument("-f", "--log_format", metavar="FORMAT", type=str, help="The format to use for logging; " + log_format_help(), default=PH_TIMESTAMP + ": " + PH_NAME)
        parser.add_argument("-o", "--output_file", metavar="FILE", type=str, help="The file to write the logging data to; " + placeholder_list(obj=self), required=False)
        parser.add_argument("-d", "--delete_on_initialize", action="store_true", help="Whether to remove any existing file when initializing the writer.")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.log_format = ns.log_format
        self.output_file = ns.output_file
        self.delete_on_initialize = ns.delete_on_initialize

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.output_file is not None:
            output_file = self.session.expand_placeholders(self.output_file)
            if os.path.exists(output_file) and os.path.isfile(output_file):
                self.logger().info("Deleting during initialization: %s" % output_file)
                os.remove(output_file)

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        now = datetime.now()
        date_str = datetime.strftime(now, FORMAT_DATE)
        time_str = datetime.strftime(now, FORMAT_TIME)
        ts_str = datetime.strftime(now, FORMAT_TS)
        for item in make_list(data):
            line = self.log_format
            line = line.replace("\\t", "\t").replace("\\n", "\n")
            if PH_DATE in line:
                line = line.replace(PH_DATE, date_str)
            if PH_TIME in line:
                line = line.replace(PH_TIME, time_str)
            if PH_TIMESTAMP in line:
                line = line.replace(PH_TIMESTAMP, ts_str)
            if isinstance(item, NameSupporter) and (PH_NAME in line):
                line = line.replace(PH_NAME, item.get_name())
            if isinstance(item, SourceSupporter) and (PH_SOURCE in line):
                line = line.replace(PH_SOURCE, item.get_source())
            if isinstance(item, AnnotationHandler):
                if PH_HAS_ANNOTATION in line:
                    line = line.replace(PH_HAS_ANNOTATION, str(item.has_annotation()))
                if PH_ANNOTATION in line:
                    line = line.replace(PH_ANNOTATION, str(item.get_annotation()))
            if isinstance(item, MetaDataHandler) and (PH_META_START in line) and item.has_metadata():
                for key in item.get_metadata().keys():
                    line = line.replace(PH_META_START + key + "}", str(item.get_metadata()[key]))
                    if PH_META_START not in line:
                        break

            if self.output_file is None:
                print(line)
            else:
                output_file = self.session.expand_placeholders(self.output_file)
                with open(output_file, "a") as fp:
                    fp.write(line)
                    fp.write("\n")

            result.append(item)

        return flatten_list(result)
