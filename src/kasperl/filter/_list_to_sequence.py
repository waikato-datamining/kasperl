import types
from typing import List

from kasperl.api import make_list, flatten_list
from seppl import AnyData
from seppl.io import Filter


class ListToSequence(Filter):
    """
    Forwards the individual items of the incoming list.
    """

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "list-to-sequence"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Forwards the individual items of the incoming list."

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

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            if isinstance(item, list):
                result.extend(item)
            elif isinstance(item, types.GeneratorType):
                for sub in item:
                    result.append(sub)
            else:
                result.append(item)

        return flatten_list(result)
