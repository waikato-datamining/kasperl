import abc
import argparse
import logging
from typing import List, Dict, Optional

from wai.logging import LOGGING_WARNING, init_logging, add_logging_level, set_logging_level
from seppl import Plugin, PluginWithLogging, Initializable, split_cmdline, split_args, args_to_objects


class Generator(PluginWithLogging, Initializable, abc.ABC):
    """
    Base class for generators.
    """

    def __init__(self, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        return None

    @abc.abstractmethod
    def _do_generate(self) -> List[Dict[str, str]]:
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        raise NotImplementedError()

    def generate(self):
        """
        Generates the variables.

        :return: the list of variable dictionaries
        :rtype: list
        """
        msg = self._check()
        if msg is not None:
            raise Exception(msg)
        return self._do_generate()

    @classmethod
    def parse_generator(cls, cmdline: str, available_generators: Dict[str, Plugin]) -> 'Generator':
        """
        Parses the commandline and returns the generator plugin.

        :param cmdline: the command-line to parse
        :type cmdline: str
        :param available_generators: the generators to use for parsing
        :type available_generators: dict
        :return: the generator plugin
        :rtype: Generator
        """
        result = cls.parse_generators(cmdline, available_generators)
        if len(result) != 1:
            raise Exception("Expected a single generator, but got: %d" % len(result))
        return result[0]

    @classmethod
    def parse_generators(cls, cmdline: str, available_generators: Dict[str, Plugin]) -> List['Generator']:
        """
        Parses the commandline and returns the list of generator plugins.

        :param cmdline: the command-line to parse
        :type cmdline: str
        :param available_generators: the generators to use for parsing
        :type available_generators: dict
        :return: the list of generator plugins
        :rtype: list
        """
        generator_args = split_args(split_cmdline(cmdline), list(available_generators.keys()))
        return args_to_objects(generator_args, available_generators)


class SingleVariableGenerator(Generator, abc.ABC):
    """
    Ancestor for generators that output just a single variable.
    """

    def __init__(self, var_name: str = None, logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the generator.

        :param var_name: the variable name
        :type var_name: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.var_name = var_name

    @abc.abstractmethod
    def _default_var_name(self) -> str:
        """
        Returns the default variable name.

        :return: the default name
        :rtype: str
        """
        raise NotImplementedError()

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-n", "--var_name", type=str, metavar="NAME", default=self._default_var_name(), help="The name of the variable", required=(self._default_var_name() is None))
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.var_name = ns.var_name

    def _check(self) -> Optional[str]:
        """
        Hook method for performing checks.

        :return: None if checks successful, otherwise error message
        :rtype: str
        """
        result = super()._check()

        if self.var_name is None:
            result = "No variable name provided!"

        return result


def test_generator(generator: str, generators: Dict[str, Plugin]):
    """
    Parses/executes the generator and then outputs the generated variables.

    :param generator: the generator command-line to use for generating variable values
    :type generator: str
    :param generators: the available generators
    :type generators: dict
    """
    # parse generator
    generator_obj = Generator.parse_generator(generator, generators)

    # apply generator to pipeline template and execute it
    vars_list = generator_obj.generate()
    for vars_ in vars_list:
        print(vars_)


def perform_generator_test(env_var: Optional[str], args: List[str], prog: str, description: Optional[str],
                           generators: Dict[str, Plugin], logger: logging.Logger):
    """
    The main method for parsing command-line arguments.

    :param env_var: the environment variable for the logging level, can be None
    :type env_var: str
    :param args: the commandline arguments, uses sys.argv if not supplied
    :type args: list
    :param prog: the name of the executable
    :type prog: str
    :param description: the description for the executable, uses default if None; list of available generators gets automatically appended
    :type description: str
    :param generators: the available generators
    :type generators: dict
    :param logger: the logger instance to use
    :type logger: logging.Logger
    """
    init_logging(env_var=env_var)
    if description is None:
        description = "Tool for testing generators by outputting the generated variables and their associated values."
    description += " Available generators: " + ", ".join(sorted(list(generators.keys())))
    parser = argparse.ArgumentParser(prog=prog, description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-g", "--generator", help="The generator plugin to use.", default=None, type=str, required=True)
    add_logging_level(parser)
    parsed = parser.parse_args(args=args)
    set_logging_level(logger, parsed.logging_level)
    test_generator(parsed.generator, generators)
