import abc
from typing import Dict, List

from seppl import Plugin, PluginWithLogging, split_cmdline, split_args, args_to_objects


class DataFormatter(PluginWithLogging, abc.ABC):

    def format_data(self, data) -> str:
        """
        Turns the data into a formatted string.

        :param data: the data to format
        :return: the generated string
        :rtype: str
        """
        raise NotImplementedError()

    @classmethod
    def parse_dataformatter(cls, cmdline: str, available_dataformatters: Dict[str, Plugin]) -> 'DataFormatter':
        """
        Parses the commandline and returns the dataformatter plugin.

        :param cmdline: the command-line to parse
        :type cmdline: str
        :param available_dataformatters: the dataformatters to use for parsing
        :type available_dataformatters: dict
        :return: the dataformatter plugin
        :rtype: DataFormatter
        """
        result = cls.parse_dataformatters(cmdline, available_dataformatters)
        if len(result) != 1:
            raise Exception("Expected a single dataformatter, but got: %d" % len(result))
        return result[0]

    @classmethod
    def parse_dataformatters(cls, cmdline: str, available_dataformatters: Dict[str, Plugin]) -> List['DataFormatter']:
        """
        Parses the commandline and returns the list of dataformatter plugins.

        :param cmdline: the command-line to parse
        :type cmdline: str
        :param available_dataformatters: the dataformatters to use for parsing
        :type available_dataformatters: dict
        :return: the list of dataformatter plugins
        :rtype: list
        """
        dataformatter_args = split_args(split_cmdline(cmdline), list(available_dataformatters.keys()))
        return args_to_objects(dataformatter_args, available_dataformatters)
