import abc
import argparse
from typing import List, Dict

from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, flatten_list, compare_values, \
    COMPARISONS_EXT, COMPARISON_EQUAL, COMPARISON_CONTAINS, COMPARISON_MATCHES, COMPARISON_EXT_HELP, \
    PIPELINE_FORMATS, PIPELINE_FORMAT_CMDLINE, load_pipeline
from seppl import split_args, Plugin, AnyData, MetaDataHandler, init_initializable, Initializable
from seppl.io import BatchFilter, MultiFilter


class SubProcess(BatchFilter, abc.ABC):
    """
    Forwards the data coming through to the sub-flow.
    """

    def __init__(self, sub_flow: str = None, sub_flow_format: str = None,
                 field: str = None, comparison: str = COMPARISON_EQUAL, value=None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the filter.

        :param sub_flow: the command-line of the filter(s) to execute
        :type sub_flow: str
        :param sub_flow_format: the format the sub_flow is in
        :type sub_flow_format: str
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
        self.sub_flow_format = sub_flow_format
        self.field = field
        self.value = value
        self.comparison = comparison
        self._sub_flow = None
        self._filter = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "sub-process"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Pushes the data through the filter(s) defined as its sub-flow. " \
               "When supplying a meta-data field and a value, this can be turned into conditional processing. " \
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
        parser.add_argument("-f", "--sub_flow", type=str, default=None, help="The subflow with filter(s) to execute.")
        parser.add_argument("-F", "--sub_flow_format", choices=PIPELINE_FORMATS, default=PIPELINE_FORMAT_CMDLINE, help="The format of the pipeline.")
        parser.add_argument("--field", type=str, help="The meta-data field to use in the comparison", default=None, required=False)
        parser.add_argument("--comparison", choices=COMPARISONS_EXT, default=COMPARISON_EQUAL, help="How to compare the value with the meta-data value; " + COMPARISON_EXT_HELP
                            + "; in case of '" + COMPARISON_CONTAINS + "' and '" + COMPARISON_MATCHES + "' the supplied value represents the substring to find/regexp to search with", required=False)
        parser.add_argument("--value", type=str, help="The value to use in the comparison", default=None, required=False)
        return parser

    @abc.abstractmethod
    def _available_filters(self) -> Dict[str, Plugin]:
        """
        Returns the available filters from the registry.

        :return: the filters
        :rtype: dict
        """
        raise NotImplementedError()

    def _parse_sub_flow(self) -> List[Plugin]:
        """
        Parses the command-line and returns the list of plugins it represents.
        Raises an exception in case of an invalid sub-flow.
        
        :return: the list of plugins
        :rtype: list
        """
        from seppl import args_to_objects

        # split command-line into valid plugin subsets
        valid = dict()
        valid.update(self._available_filters())
        pipeline = load_pipeline(self.sub_flow, self.sub_flow_format, logger=self.logger())
        args = split_args(pipeline, list(valid.keys()))
        return args_to_objects(args, valid, allow_global_options=False)

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.sub_flow = ns.sub_flow
        self.sub_flow_format = ns.sub_flow_format
        self.field = ns.field
        self.value = ns.value
        self.comparison = ns.comparison

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()

        if self.sub_flow is None:
            self._sub_flow = []
        else:
            self._sub_flow = self._parse_sub_flow()

        if len(self._sub_flow) > 0:
            self._filter = None
            filters = []
            for plugin in self._sub_flow:
                if isinstance(plugin, BatchFilter):
                    filters.append(plugin)
            if len(filters) == 1:
                self._filter = filters[0]
            elif len(filters) > 1:
                self._filter = MultiFilter(filters=filters)

        if self._filter is not None:
            self._filter.session = self.session
            if isinstance(self._filter, Initializable):
                init_initializable(self._filter, "filter")

        if (self.field is not None) and (self.value is None):
            raise Exception("No value provided to compare with!")

    def _do_process(self, data):
        """
        Processes the data record(s).

        :param data: the record(s) to process
        :return: the potentially updated record(s)
        """
        result = []
        for item in make_list(data):
            process = True

            # evaluate expression?
            meta = None
            if self.field is not None:
                if isinstance(item, MetaDataHandler):
                    if item.has_metadata():
                        meta = item.get_metadata()
            if meta is not None:
                v1 = meta[self.field]
                v2 = self.value
                process = compare_values(v1, self.comparison, v2)
                comp = str(meta[self.field]) + " " + self.comparison + " " + str(self.value) + " = " + str(process)
                self.logger().info("Field '%s': '%s'" % (self.field, comp))

            # filter data
            if process and (self._filter is not None):
                item = self._filter.process(item)

            result.append(item)

        return flatten_list(result)

    def finalize(self):
        """
        Finishes the processing, e.g., for closing files or databases.
        """
        super().finalize()

        # finalize sub-flow
        if (self._filter is not None) and isinstance(self._filter, Initializable):
            self._filter.finalize()
