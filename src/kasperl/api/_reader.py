import abc
import argparse
from typing import Dict, List, Iterable

from wai.logging import LOGGING_WARNING

import seppl.io
from seppl import Initializable, Plugin, AnyData, init_initializable


class Reader(seppl.io.Reader, Initializable, abc.ABC):
    """
    Ancestor for dataset readers.
    """
    pass


def parse_reader(reader: str, available_readers: Dict[str, Plugin]) -> Reader:
    """
    Parses the command-line and instantiates the reader.

    :param reader: the command-line to parse
    :type reader: str
    :param available_readers: the available readers to use for parsing
    :type available_readers: dict
    :return: the reader instance
    :rtype: Reader
    """
    from seppl import split_args, split_cmdline, args_to_objects

    if reader is None:
        raise Exception("No reader command-line supplied!")
    valid = dict()
    valid.update(available_readers)
    args = split_args(split_cmdline(reader), list(valid.keys()))
    objs = args_to_objects(args, valid, allow_global_options=False)
    if len(objs) == 1:
        if isinstance(objs[0], Reader):
            result = objs[0]
        else:
            raise Exception("Expected instance of Reader but got: %s" % str(type(objs[0])))
    else:
        raise Exception("Expected to obtain one reader from '%s' but got %d instead!" % (reader, len(objs)))
    return result


def add_annotations_only_reader_param(parser: argparse.ArgumentParser):
    """
    Adds the --annotations_only parameter to the parser, as used by readers of type AnnotationOnlyReader.

    :param parser: the parser
    :type parser: argparse.ArgumentParser
    """
    parser.add_argument("--annotations_only", action="store_true", help="Reads only the annotations.")


class AnnotationsOnlyReader:
    """
    Mixin for reader that can read the annotations by itself.
    """
    pass


class MetaFileReader(Reader, abc.ABC):
    """
    Ancestor for classes that use a base reader for doing the actual reading
    of the files that the reader determined.
    """

    def __init__(self, base_reader: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param base_reader: the base reader to use (command-line)
        :type base_reader: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.base_reader = base_reader
        self._base_reader = None

    def _base_reader_help(self) -> str:
        """
        Returns the help string on the base reader for the argument parser.

        :return: the help string
        :rtype: str
        """
        return "The command-line of the reader for reading the files"

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-b", "--base_reader", type=str, help=self._base_reader_help(), required=True, default=None)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.base_reader = ns.base_reader

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        if self._base_reader is None:
            return [AnyData]
        else:
            return [self._base_reader.generates()]

    @abc.abstractmethod
    def _available_readers(self) -> Dict[str, Plugin]:
        """
        Return the available readers.

        :return: the reader plugins
        :rtype: dict
        """
        raise NotImplementedError()

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """

        super().initialize()

        if self.base_reader is None:
            raise Exception("No base reader defined!")

        # configure base reader
        self._base_reader = parse_reader(self.base_reader, self._available_readers())
        if not hasattr(self._base_reader, "source"):
            raise Exception("Reader does not have 'source' attribute: %s" % str(type(self._base_reader)))
        self._base_reader.session = self.session

    def _read_files(self, files: List[str]) -> Iterable:
        """
        Reads the files with the base reader and returns the results.

        :param files: the files to read
        :type files: list
        :return: the data that was generated
        """
        result = []
        self._base_reader.source = files
        if isinstance(self._base_reader, Initializable):
            init_initializable(self._base_reader, "reader", raise_again=True)
        while not self._base_reader.has_finished():
            for item in self._base_reader.read():
                result.append(item)
        self._base_reader.finalize()
        return result

    def finalize(self):
        """
        Finishes the reading, e.g., for closing files or databases.
        """
        super().finalize()
        if self._base_reader is not None:
            self._base_reader.finalize()
            self._base_reader = None
