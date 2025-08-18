from typing import Dict
from seppl import Plugin
from seppl.io import Filter


def parse_filter(filter_: str, available_filters: Dict[str, Plugin]) -> Filter:
    """
    Parses the command-line and instantiates the filter.

    :param filter_: the command-line to parse
    :type filter_: str
    :param available_filters: the available filters to use for parsing
    :type available_filters: dict
    :return: the filter instance
    :rtype: Filter
    """
    from seppl import split_args, split_cmdline, args_to_objects

    if filter_ is None:
        raise Exception("No filter command-line supplied!")
    valid = dict()
    valid.update(available_filters)
    args = split_args(split_cmdline(filter_), list(valid.keys()))
    objs = args_to_objects(args, valid, allow_global_options=False)
    if len(objs) == 1:
        if isinstance(objs[0], Filter):
            result = objs[0]
        else:
            raise Exception("Expected instance of Filter but got: %s" % str(type(objs[0])))
    else:
        raise Exception("Expected to obtain one filter from '%s' but got %d instead!" % (filter_, len(objs)))
    return result
