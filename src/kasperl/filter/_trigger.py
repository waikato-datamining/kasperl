import abc
import argparse
from typing import List, Dict

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list, compare_values, \
    COMPARISONS_EXT, COMPARISON_EQUAL, COMPARISON_CONTAINS, COMPARISON_MATCHES, COMPARISON_EXT_HELP
from seppl import split_args, split_cmdline, Plugin, AnyData, Initializable, MetaDataHandler, init_initializable
from seppl.io import Writer, BatchWriter, BatchFilter, MultiFilter, Reader, execute


class Trigger(BatchFilter, abc.ABC):
    """
    Triggers the sub-flow with data passing through.
    """

    def __init__(self, sub_flow: List[Plugin] = None,
                 field: str = None, comparison: str = COMPARISON_EQUAL, value=None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param sub_flow: the reader/filter(s)/writer to execute
        :type sub_flow: list
        :param field: the name of the meta-data field to perform the comparison on
        :type field: str
        :param comparison: the comparison to perform
        :type comparison: str
        :param value: the value to compare with
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.sub_flow = sub_flow
        self.field = field
        self.value = value
        self.comparison = comparison
        self._reader = None
        self._filter = None
        self._writer = None
        self._data_buffer = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "trigger"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Triggers the sub-flow with its reader/filter(s)/writer whenever data is passing through. " \
               "When supplying a meta-data field and a value, this can be turned into a conditional execution, but only when receiving individual objects, not batches of objects. " \
               "Performs the following comparison: METADATA_VALUE COMPARISON VALUE."

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
        parser.add_argument("-f", "--sub_flow", type=str, default=None, help="The command-line defining the subflow (reader/filter(s)/writer).")
        parser.add_argument("--field", type=str, help="The meta-data field to use in the comparison", default=None, required=False)
        parser.add_argument("--comparison", choices=COMPARISONS_EXT, default=COMPARISON_EQUAL, help="How to compare the value with the meta-data value; " + COMPARISON_EXT_HELP
                            + "; in case of '" + COMPARISON_CONTAINS + "' and '" + COMPARISON_MATCHES + "' the supplied value represents the substring to find/regexp to search with", required=False)
        parser.add_argument("--value", type=str, help="The value to use in the comparison", default=None, required=False)
        return parser

    @abc.abstractmethod
    def _available_readers(self) -> Dict[str, Plugin]:
        """
        Returns the available readers from the registry.

        :return: the readers
        :rtype: dict
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _available_filters(self) -> Dict[str, Plugin]:
        """
        Returns the available filters from the registry.

        :return: the filters
        :rtype: dict
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _available_writers(self) -> Dict[str, Plugin]:
        """
        Returns the available writers from the registry.

        :return: the writers
        :rtype: dict
        """
        raise NotImplementedError()

    def _parse_commandline(self, cmdline: str) -> List[Plugin]:
        """
        Parses the command-line and returns the list of plugins it represents.
        Raises an exception in case of an invalid sub-flow.
        
        :param cmdline: the command-line to parse
        :type cmdline: str 
        :return: 
        """
        from seppl import args_to_objects

        # split command-line into valid plugin subsets
        valid = dict()
        valid.update(self._available_readers())
        valid.update(self._available_filters())
        valid.update(self._available_writers())
        args = split_args(split_cmdline(cmdline), list(valid.keys()))
        return args_to_objects(args, valid, allow_global_options=False)

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.sub_flow = None
        if ns.sub_flow is not None:
            self.sub_flow = self._parse_commandline(ns.sub_flow)
        self.field = ns.field
        self.value = ns.value
        self.comparison = ns.comparison

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()

        if self.sub_flow is None:
            self.sub_flow = []

        if len(self.sub_flow) > 0:
            self._reader = None
            self._filter = None
            self._writer = None
            filters = []
            for plugin in self.sub_flow:
                if isinstance(plugin, Reader):
                    if self._reader is None:
                        self._reader = plugin
                    else:
                        raise Exception("More than one reader defined in sub-flow!")
                elif isinstance(plugin, BatchFilter):
                    filters.append(plugin)
                elif isinstance(plugin, Writer):
                    self._writer = plugin
                    # no more plugins allowed beyond writer
                    break
            if len(filters) == 1:
                self._filter = filters[0]
            elif len(filters) > 1:
                self._filter = MultiFilter(filters=filters)
            if (self._writer is not None) and isinstance(self._writer, BatchWriter):
                self._data_buffer = []

        if self._reader is None:
            raise Exception("No reader defined in sub-flow!")
        else:
            self._reader.session = self.session
            if isinstance(self._reader, Initializable):
                init_initializable(self._reader, "reader")
        if self._filter is not None:
            self._filter.session = self.session
            if isinstance(self._filter, Initializable):
                init_initializable(self._filter, "filter")
        if self._writer is not None:
            self._writer.session = self.session
            if isinstance(self._writer, Initializable):
                init_initializable(self._writer, "writer")

        if (self.field is not None) and (self.value is None):
            raise Exception("No value provided to compare with!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = make_list(data)

        # evaluate expression?
        if len(result) == 1:
            meta = None
            if self.field is not None:
                item = result[0]
                if isinstance(item, MetaDataHandler):
                    if item.has_metadata():
                        meta = item.get_metadata()
            if meta is not None:
                v1 = meta[self.field]
                v2 = self.value
                comp_result = compare_values(v1, self.comparison, v2)
                comp = str(meta[self.field]) + " " + self.comparison + " " + str(self.value) + " = " + str(comp_result)
                self.logger().info("Field '%s': '%s'" % (self.field, comp))
                if not comp_result:
                    return result

        # trigger sub-flow (initializes/finalizes automatically)
        execute(self._reader, self._filter, self._writer, self.session)

        return flatten_list(result)
