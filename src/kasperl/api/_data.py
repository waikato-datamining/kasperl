from typing import List


def make_list(data, cls=list) -> List:
    """
    Wraps the data item in a list if not already a list.

    :param data: the data item to wrap if necessary
    :param cls: the type of class to check for
    :return: the list object
    :rtype: list
    """
    if not isinstance(data, cls):
        data = [data]
    return data


def flatten_list(data: List):
    """
    If the list contains only a single item, then it returns that instead of a list.

    :param data: the list to check
    :type data: list
    :return: the list or single item
    """
    if len(data) == 1:
        return data[0]
    else:
        return data
