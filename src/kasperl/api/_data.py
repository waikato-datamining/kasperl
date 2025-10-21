import types
from typing import List, Any


def make_list(data, cls=list) -> List:
    """
    Wraps the data item in a list if not already a list. Generators get turned into lists automatically.

    :param data: the data item to wrap if necessary
    :param cls: the type of class to check for
    :return: the list object
    :rtype: list
    """
    if isinstance(data, types.GeneratorType):
        return list(data)
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


class AnnotationHandler:
    """
    Mixin for classes that manage annotations.
    """

    def has_annotation(self) -> bool:
        """
        Checks whether annotations are present.

        :return: True if annotations present
        :rtype: bool
        """
        raise NotImplementedError()

    def get_annotation(self) -> Any:
        """
        Returns the annotations.

        :return: the annotations
        """
        raise NotImplementedError()

    def set_annotation(self, ann: Any):
        """
        Sets the annotations.

        :param ann: the annotations
        """
        raise NotImplementedError()


class NameSupporter:
    """
    Mixin for classes that manage a name.
    """

    def get_name(self) -> str:
        """
        Returns the name.

        :return: the name
        :rtype: str
        """
        raise NotImplementedError()

    def set_name(self, name: str):
        """
        Sets the new name.

        :param name: the new name
        :type name: str
        """
        raise NotImplementedError()


class SourceSupporter:
    """
    Mixin for classes that manage a source, e.g., a file name.
    """

    def get_source(self) -> str:
        """
        Returns the source.

        :return: the source
        :rtype: str
        """
        raise NotImplementedError()
