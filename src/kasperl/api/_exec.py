import argparse
import logging
import os
import shlex
from typing import List, Dict, Optional, Union

from wai.logging import init_logging, set_logging_level, add_logging_level

from seppl import split_cmdline, Plugin, load_args, remove_comments
from seppl.placeholders import load_user_defined_placeholders, expand_placeholders
from ._generator import compile_generator_vars_list
from ._help import CommandlineParameter, params_to_parser


PARAM_EXEC_FORMAT = "--exec_format"


DESCRIPTION = "Tool for executing a pipeline multiple times, each time with a different set of variables expanded. " \
              + "A variable is surrounded by curly quotes (e.g., variable 'i' gets referenced with '{i}'). " \
              + "When supplying multiple generators, then these get treated as nested executions."


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
            result = remove_comments(pipeline)

    elif pipeline_format == PIPELINE_FORMAT_FILE:
        if isinstance(pipeline, str):
            pipeline_file = pipeline
        else:
            pipeline_file = shlex.join(pipeline)
        pipeline_file = expand_placeholders(pipeline_file)
        result = load_args(pipeline_file, logger=logger)

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


def execute_pipeline(convert_prog: str, convert_func, pipeline: Union[str, List[str]],
                     generator: Union[str, List[str]], generator_plugins: Dict[str, Plugin],
                     pipeline_format: str = PIPELINE_FORMAT_CMDLINE, dry_run: bool = False,
                     prefix: str = False, logger: logging.Logger = None):
    """
    Executes the specified pipeline as many times as the generators produce variables.

    :param convert_prog: the conversion executable
    :type convert_prog: str
    :param convert_func: the main method for executing the conversion executable
    :param pipeline: the pipeline template to use
    :type pipeline: str or list
    :param generator: the generator command-line(s) to use for generating variable values to be expanded in the pipeline template
    :type generator: str or list
    :param generator_plugins: the available generator plugins (name -> plugin)
    :type generator_plugins: dict
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

    # generate variables
    vars_list = compile_generator_vars_list(generator, generator_plugins)

    # apply generator to pipeline template and execute it
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
                               convert_prog: str, convert_func, generators_plugins: Dict[str, Plugin],
                               logger: logging.Logger, additional_params: Optional[List[CommandlineParameter]] = None,
                               pre_exec=None, post_exec=None):
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
    :param generators_plugins: the available generators
    :type generators_plugins: dict
    :param logger: the logger instance to use
    :type logger: logging.Logger
    :param additional_params: the additional parameters for the parser
    :type additional_params: list
    :param pre_exec: the optional method to execute before the pipeline executions, gets parsed args namespace as only argument
    :param post_exec: the optional method to execute after the pipeline got executed, gets parsed args namespace as only argument
    """
    init_logging(env_var=env_var)

    if description is None:
        description = DESCRIPTION
    description += " Available generators: " + ", ".join(generators_plugins)
    parser = argparse.ArgumentParser(
        prog=prog, description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--exec_generator", metavar="GENERATOR", help="The generator plugin(s) to use, incl. their options. Flag needs to be specified for each generator.", default=None, type=str, required=True, action="append")
    parser.add_argument("--exec_dry_run", action="store_true", help="Applies the generator to the pipeline template and only outputs it on stdout.", required=False)
    parser.add_argument("--exec_prefix", metavar="PREFIX", help="The string to prefix the pipeline with when in dry-run mode.", required=False, default=None, type=str)
    parser.add_argument("--exec_placeholders", metavar="FILE", help="The file with custom placeholders to load (format: key=value).", required=False, default=None, type=str)
    parser.add_argument(PARAM_EXEC_FORMAT, choices=PIPELINE_FORMATS, required=False, default=PIPELINE_FORMAT_CMDLINE,
                        help="The format that the pipeline is in. "
                             + "The format '" + PIPELINE_FORMAT_CMDLINE + "' interprets the remaining "
                             + "arguments as the pipeline arguments to execute. "
                             + "The format '" + PIPELINE_FORMAT_FILE + "' expects a file to load the pipeline arguments "
                             + "from. This file format allows spreading the pipeline arguments over multiple lines: it "
                             + "simply joins all lines into a single command-line before splitting it into individual "
                             + "arguments for execution.")
    parser.add_argument("pipeline", help="The pipeline template with variables to expand and then execute; see '" + PARAM_EXEC_FORMAT + "' option.", nargs=argparse.REMAINDER)
    if logger is not None:
        add_logging_level(parser, short_opt=None, long_opt="--exec_logging_level")
    params_to_parser(parser, additional_params)

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

    # custom pre-execution hook?
    if pre_exec is not None:
        pre_exec(parsed)

    execute_pipeline(convert_prog, convert_func, parsed.pipeline, parsed.exec_generator, generators_plugins,
                     pipeline_format=parsed.exec_format, dry_run=parsed.exec_dry_run, prefix=parsed.exec_prefix,
                     logger=logger)

    # custom post-execution hook?
    if post_exec is not None:
        post_exec(parsed)
