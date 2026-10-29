import argparse
from typing import List

from wai.logging import LOGGING_WARNING

from seppl import AnyData, MetaDataHandler
from seppl.io import BatchFilter
from kasperl.api import make_list, flatten_list, AnnotationHandler


class AnnotationsToMetadata(BatchFilter):
    """
    Stores the annotations in the metadata under the specified field.
    """

    def __init__(self, metadata_key: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param metadata_key: the field in the meta-data to store the annotations under
        :type metadata_key: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.metadata_key = metadata_key

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "annotations-to-metadata"

    def description(self) -> str:
        """
        Returns a description of the filter.

        :return: the description
        :rtype: str
        """
        return "Stores the annotations in the metadata under the specified field."

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
        parser.add_argument("-s", "--metadata_key", type=str, help="The field to store the annotations under in the meta-data.", required=False, default="annotations")
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.metadata_key = ns.metadata_key

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            annotation = None
            meta = dict()
            add = True

            # meta-data
            if not isinstance(item, MetaDataHandler):
                self.logger().warning("Does not manage meta-data: %s" % str(type(item)))
                add = False
            else:
                if item.has_metadata():
                    meta = meta.get_metadata()

            # annotations
            if add:
                if not isinstance(item, AnnotationHandler):
                    self.logger().warning("Does not manage annotations: %s" % str(type(item)))
                    add = False
                else:
                    add = item.has_annotation()
                    annotation = item.get_annotation()

            if add:
                meta[self.metadata_key] = annotation
                item.set_metadata(meta)

            result.append(item)

        return flatten_list(result)
