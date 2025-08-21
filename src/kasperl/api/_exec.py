import argparse
import logging
import os
import shlex
from typing import List, Dict, Optional, Union

from wai.logging import init_logging, set_logging_level, add_logging_level

from kasperl.api import Generator
from seppl import split_cmdline, Plugin
from seppl.placeholders import load_user_defined_placeholders


DESCRIPTION = "Tool for executing a pipeline multiple times, each time with a different set of variables expanded. " \
              "A variable is surrounded by curly quotes (e.g., variable 'i' gets referenced with '{i}'). " \
              "All remaining arguments are interpreted as pipeline arguments, making it easy to prefix the exec " \
              "arguments to an existing pipeline command."


def _expand_vars(pipeline: Union[str, List[str]], vars_: Dict) -> Union[str, List[str]]:
    """
    Expands the variables in the pipeline.

    :param pipeline: the pipeline to expand
    :type pipeline: str or list
    :param vars_: the variables to expand
    :type vars_: dict
    :return: the expanded pipeline
    :rtype: str or list
    """
    if isinstance(pipeline, str):
        result = pipeline
        for var in vars_:
            result = result.replace("{%s}" % var, vars_[var])
    else:
        result = []
        for arg in pipeline:
            for var in vars_:
                arg = arg.replace("{%s}" % var, vars_[var])
            result.append(arg)

    return result


def execute_pipeline(convert_prog: str, convert_func, pipeline: Union[str, List[str]], generator: str, generators: Dict[str, Plugin],
                     dry_run: bool = False, prefix: str = False, logger: logging.Logger = None):
    """
    Executes the specified pipeline as many times as the generators produce variables.

    :param convert_prog: the conversion executable
    :type convert_prog: str
    :param convert_func: the main method for executing the conversion executable
    :param pipeline: the pipeline template to use
    :type pipeline: str or list
    :param generator: the generator command-line to use for generating variable values to be expanded in the pipeline template
    :type generator: str
    :param generators: the available generators
    :type generators: dict
    :param dry_run: whether to only expand/output but not execute the pipeline
    :type dry_run: bool
    :param prefix: the prefix to use when in dry-run mode
    :type prefix: str
    :param logger: the logger instance to use
    :type logger: logging.Logger
    """
    if isinstance(pipeline, str):
        # remove whitespaces, idc-convert from pipeline
        pipeline = pipeline.strip()
        if pipeline.startswith(convert_prog):
            pipeline = pipeline[len(convert_prog):].strip()
    else:
        # remove idc-convert from pipeline
        if len(pipeline) > 0:
            if pipeline[0].startswith(convert_prog):
                pipeline = pipeline[1:]

    # parse generator
    generator_obj = Generator.parse_generator(generator, generators)

    # apply generator to pipeline template and execute it
    vars_list = generator_obj.generate()
    for vars_ in vars_list:
        pipeline_exp = _expand_vars(pipeline, vars_)
        if logger is not None:
            logger.info("%s\n--> %s" % (str(pipeline), str(pipeline_exp)))
        if dry_run:
            if prefix is not None:
                if not prefix.endswith(" "):
                    prefix = prefix + " "
                if isinstance(pipeline_exp, str):
                    pipeline_exp = prefix + pipeline_exp
                    print(pipeline_exp)
                else:
                    pipeline_exp[0] = prefix + pipeline_exp[0]
                    print(shlex.join(pipeline_exp))
        else:
            if isinstance(pipeline_exp, str):
                pipeline_args = split_cmdline(pipeline_exp)
            else:
                pipeline_args = pipeline_exp
            convert_func(pipeline_args)


def perform_pipeline_execution(env_var: Optional[str], args: List[str], prog: str, description: Optional[str],
                               convert_prog: str, convert_func, generators: Dict[str, Plugin],
                               logger: logging.Logger):
    """
    The main method for parsing command-line arguments.

    :param env_var: the environment variable to obtain the logging level from, can be None
    :type env_var: str
    :param args: the commandline arguments, uses sys.argv if not supplied
    :type args: list
    :param prog: the executable name
    :type prog: str
    :param description: the description for the executable, uses default if None; automatically appends available generators
    :type description: str
    :param convert_prog: the conversion executable
    :type convert_prog: str
    :param convert_func: the main method for executing the conversion executable
    :param generators: the available generators
    :type generators: dict
    :param logger: the logger instance to use
    :type logger: logging.Logger
    """
    init_logging(env_var=env_var)
    if description is None:
        description = DESCRIPTION
    description += " Available generators: " + ", ".join(generators)
    parser = argparse.ArgumentParser(
        prog=prog, description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-g", "--generator", help="The generator plugin to use.", default=None, type=str, required=True)
    parser.add_argument("-n", "--dry_run", action="store_true", help="Applies the generator to the pipeline template and only outputs it on stdout.", required=False)
    parser.add_argument("-P", "--prefix", help="The string to prefix the pipeline with when in dry-run mode.", required=False, default=None, type=str)
    parser.add_argument("--placeholders", metavar="FILE", help="The file with custom placeholders to load (format: key=value).", required=False, default=None, type=str)
    parser.add_argument("pipeline", help="The pipeline template with variables to expand and then execute.", nargs=argparse.REMAINDER)
    if logger is not None:
        add_logging_level(parser)
    parsed = parser.parse_args(args=args)
    if logger is not None:
        set_logging_level(logger, parsed.logging_level)
    if parsed.placeholders is not None:
        if not os.path.exists(parsed.placeholders):
            msg = "Placeholder file not found: %s" % parsed.placeholders
            if logger is not None:
                logger.error(msg)
            else:
                print(msg)
        else:
            msg = "Loading custom placeholders from: %s" % parsed.placeholders
            if logger is not None:
                logger.info(msg)
            else:
                print(msg)
            load_user_defined_placeholders(parsed.placeholders)
    execute_pipeline(convert_prog, convert_func, parsed.pipeline, parsed.generator, generators,
                     dry_run=parsed.dry_run, prefix=parsed.prefix, logger=logger)
