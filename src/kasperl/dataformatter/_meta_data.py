import argparse

from wai.logging import LOGGING_WARNING

from kasperl.api import DataFormatter

PH_DATA = "data"
PH_HAS_METADATA = "has-metadata"
PH_METADATA_START = "metadata:"
PLACEHOLDERS = [
    PH_DATA,
    PH_HAS_METADATA,
]


class MetaDataFormatter(DataFormatter):

    def __init__(self, output_format: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param output_format: the format for the output
        :type output_format: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.output_format = output_format

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "df-meta-data"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Expands the placeholders in the format string using the available meta-data."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-f", "--output_format", type=str, help="The format to use for the output, available placeholders: %s" % ", ".join(PLACEHOLDERS) + "; for metadata fields use " + PH_METADATA_START + "<fieldname>", required=False, default="{" + PH_DATA + "}")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.output_format = ns.output_format

    def format_data(self, data) -> str:
        """
        Turns the data into a formatted string.

        :param data: the data to format
        :return: the generated string
        :rtype: str
        """
        result = self.output_format

        for ph in PLACEHOLDERS:
            ph_full = "{" + ph + "}"
            if ph_full in result:
                value = None
                if ph == PH_DATA:
                    value = str(data)
                elif ph == PH_HAS_METADATA:
                    value = str(data.has_metadata())
                if value is None:
                    self.logger().warning("Unsupported placeholder: {%s}" % ph_full)
                else:
                    result = result.replace(ph_full, value)

        if PH_METADATA_START in result:
            for field in data.get_metadata():
                ph_full = "{" + PH_METADATA_START + field + "}"
                value = data.get_metadata()[field]
                result = result.replace(ph_full, str(value))

        return result
