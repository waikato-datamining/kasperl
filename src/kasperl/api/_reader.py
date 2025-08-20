import abc
import argparse
from typing import Dict
import seppl.io
from seppl import Initializable, Plugin


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
