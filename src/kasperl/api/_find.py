import logging
import os
import re
from typing import List

from seppl.io import Splitter


def _find(path: str, recursive: bool, match: List[str], not_match: List[str], files: List[str],
          logger: logging.Logger = None):
    """
    Locates files in the specified input directory and adds the matches to the files list.

    :param path: the directory to scan
    :type path: str
    :param recursive: whether to scan the dir(s) recursively
    :type recursive: bool
    :param match: the list regexp that the full paths of the files must match in order to be included (None: includes all)
    :type match: list
    :param not_match: the regexp that the full paths of the files must match in order to be excluded (None: includes all)
    :type not_match: list
    :param files: the list to add the matching files to
    :type files: list
    :param logger: the optional logger instance to use
    :type logger: logging.Logger
    """
    if logger is not None:
        logger.info("Entering: %s" % path)
    for f in os.listdir(path):
        if (f == ".") or (f == ".."):
            continue

        full = os.path.join(path, f)

        # dir?
        if os.path.isdir(full):
            if recursive:
                _find(full, recursive, match, not_match, files)
                logger.info("Back to: %s" % path)
            else:
                continue

        # match?
        is_match = True
        if match is not None:
            for pattern in match:
                if re.search(pattern, full) is None:
                    is_match = False
                    break
        if is_match and (not_match is not None):
            for pattern in not_match:
                if re.search(pattern, full) is not None:
                    is_match = False
                    break
        if is_match:
            files.append(full)


def find_files(paths: List[str], output: str, recursive=False,
               match: List[str] = None, not_match: List[str] = None,
               split_ratios: List[int] = None, split_names: List[str] = None,
               split_name_separator: str = "-", logger: logging.Logger = None):
    """
    Finds the files in the dir(s) and stores the full paths in text file(s).

    :param paths: the dir(s) to scan for files
    :type paths: list
    :param output: the output file to store the files in; when splitting, the split names get used as suffix (before .ext)
    :type output: str
    :param recursive: whether to scan the dir(s) recursively
    :type recursive: bool
    :param match: the list regexp that the full paths of the files must match in order to be included (None: includes all)
    :type match: list
    :param not_match: the regexp that the full paths of the files must match in order to be excluded (None: includes all)
    :type not_match: list
    :param split_ratios: the list of (int) ratios to use for splitting (must sum up to 100)
    :type split_ratios: list
    :param split_names: the list of suffixes to use for the splits
    :type split_names: list
    :param split_name_separator: the separator to use between file name and split name
    :type split_name_separator: str
    :param logger: the optional logger instance to use
    :type logger: logging.Logger
    """
    # locate
    all_files = []
    for d in paths:
        _find(d, recursive, match, not_match, all_files)
    if logger is not None:
        logger.info("# files found: %d" % len(all_files))

    # split?
    if (split_ratios is not None) or (split_names is not None):
        splitter = Splitter(split_ratios, split_names)
        splitter.initialize()
        # generate output names
        split_output = dict()
        parts = os.path.splitext(output)
        for name in split_names:
            split_output[name] = parts[0] + split_name_separator + name + parts[1]
        # split the files
        split_files = dict()
        for name in split_names:
            split_files[name] = []
        for f in all_files:
            split_files[splitter.next()].append(f)
        for name in split_names:
            if logger is not None:
                logger.info("# files in split '%s': %d" % (name, len(split_files[name])))
        # write files
        for name in split_names:
            if logger is not None:
                logger.info("Writing files to: %s" % split_output[name])
            with open(split_output[name], "w") as fp:
                for f in split_files[name]:
                    fp.write(f)
                    fp.write("\n")
    else:
        if logger is not None:
            logger.info("Writing files to: %s" % output)
        with open(output, "w") as fp:
            for f in all_files:
                fp.write(f)
                fp.write("\n")
