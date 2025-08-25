import argparse
import logging
import os
import shlex
from typing import List, Dict, Optional, Union

from wai.logging import init_logging, set_logging_level, add_logging_level

from seppl import split_cmdline, Plugin
from seppl.placeholders import load_user_defined_placeholders, expand_placeholders
from kasperl.api import Generator


DESCRIPTION = "Tool for executing a pipeline multiple times, each time with a different set of variables expanded. " \
              "A variable is surrounded by curly quotes (e.g., variable 'i' gets referenced with '{i}'). " \
              "All remaining arguments are interpreted as pipeline arguments, making it easy to prefix the exec " \
              "arguments to an existing pipeline command."


PIPELINE_FORMAT_CMDLINE = "cmdline"
PIPELINE_FORMAT_FILE = "file"
PIPELINE_FORMATS = [
    PIPELINE_FORMAT_CMDLINE,
    PIPELINE_FORMAT_FILE,
]


def load_pipeline(pipeline: Union[str, List[str]], pipeline_format: str = PIPELINE_FORMAT_CMDLINE,
                  remove_convert_prog: bool = False, convert_prog: str = None, logger: logging.Logger = None) -> List[str]:
    """
    Loads the pipeline according to the format and returns the split arguments.

    :param pipeline: the pipeline
    :param pipeline_format: the format of the pipeline
    :type pipeline_format: str
    :param remove_convert_prog: whether to remove the convert program from the arguments
    :type remove_convert_prog: bool
    :param convert_prog: the executable to potentially remove
    :type convert_prog: str
    :param logger: the optional logger to use
    :type logger: logging.Logger
    :return: the split arguments of the pipeline
    :rtype: list
    """
    if pipeline_format == PIPELINE_FORMAT_CMDLINE:
        if isinstance(pipeline, str):
            result = split_cmdline(pipeline)
        else:
            result = pipeline

    elif pipeline_format == PIPELINE_FORMAT_FILE:
        if isinstance(pipeline, str):
            pipeline_file = pipeline
        else:
            pipeline_file = shlex.join(pipeline)
        pipeline_file = expand_placeholders(pipeline_file)
        if logger is not None:
            logger.info("Loading pipeline from: %s" % pipeline_file)
        with open(pipeline_file, "r") as fp:
            lines = fp.readlines()
        lines = [x.strip() for x in lines]
        result = split_cmdline(" ".join(lines))

    else:
        raise Exception("Unhandled pipeline format: %s" % pipeline_format)

    # remove convert prog from pipeline
    if remove_convert_prog:
        if len(result) > 0:
            if result[0].startswith(convert_prog):
                result = result[1:]

    if logger is not None:
        logger.info("Pipeline: %s" % str(result))

    return result


def _expand_vars(pipeline: List[str], vars_: Dict) -> Union[str, List[str]]:
    """
    Expands the variables in the pipeline.

    :param pipeline: the pipeline to expand
    :type pipeline: list
    :param vars_: the variables to expand
    :type vars_: dict
    :return: the expanded pipeline
    :rtype: str or list
    """
    result = []
    for arg in pipeline:
        for var in vars_:
            arg = arg.replace("{%s}" % var, vars_[var])
        result.append(arg)

    return result


def execute_pipeline(convert_prog: str, convert_func, pipeline: Union[str, List[str]], generator: str, generators: Dict[str, Plugin],
                     pipeline_format: str = PIPELINE_FORMAT_CMDLINE, dry_run: bool = False, prefix: str = False, logger: logging.Logger = None):
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
    :param pipeline_format: the format the pipeline is in
    :type pipeline_format: str
    :param dry_run: whether to only expand/output but not execute the pipeline
    :type dry_run: bool
    :param prefix: the prefix to use when in dry-run mode
    :type prefix: str
    :param logger: the logger instance to use
    :type logger: logging.Logger
    """
    # load pipeline
    pipeline = load_pipeline(pipeline, pipeline_format=pipeline_format,
                             remove_convert_prog=True, convert_prog=convert_prog, logger=logger)

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
                pipeline_exp[0] = prefix + pipeline_exp[0]
                print(shlex.join(pipeline_exp))
        else:
            convert_func(pipeline_exp)


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
    parser.add_argument("--exec_generator", metavar="GENERATOR", help="The generator plugin to use, incl. its options.", default=None, type=str, required=True)
    parser.add_argument("--exec_dry_run", action="store_true", help="Applies the generator to the pipeline template and only outputs it on stdout.", required=False)
    parser.add_argument("--exec_prefix", metavar="PREFIX", help="The string to prefix the pipeline with when in dry-run mode.", required=False, default=None, type=str)
    parser.add_argument("--exec_placeholders", metavar="FILE", help="The file with custom placeholders to load (format: key=value).", required=False, default=None, type=str)
    parser.add_argument("--exec_format", choices=PIPELINE_FORMATS, help="The format that the pipeline is in.", required=False, default=PIPELINE_FORMAT_CMDLINE)
    parser.add_argument("pipeline", help="The pipeline template with variables to expand and then execute. The format '" + PIPELINE_FORMAT_FILE + "' allows spreading the pipeline over multiple lines: it simply joins all lines into single command-line before splitting into individual arguments.", nargs=argparse.REMAINDER)
    if logger is not None:
        add_logging_level(parser, short_opt=None, long_opt="--exec_logging_level")

    parsed = parser.parse_args(args=args)

    if logger is not None:
        set_logging_level(logger, parsed.logging_level)

    if parsed.exec_placeholders is not None:
        if not os.path.exists(parsed.exec_placeholders):
            msg = "Placeholder file not found: %s" % parsed.exec_placeholders
            if logger is not None:
                logger.error(msg)
            else:
                print(msg)
        else:
            msg = "Loading custom placeholders from: %s" % parsed.exec_placeholders
            if logger is not None:
                logger.info(msg)
            else:
                print(msg)
            load_user_defined_placeholders(parsed.exec_placeholders)

    execute_pipeline(convert_prog, convert_func, parsed.pipeline, parsed.exec_generator, generators,
                     pipeline_format=parsed.exec_format, dry_run=parsed.exec_dry_run, prefix=parsed.exec_prefix,
                     logger=logger)
