import argparse
from dataclasses import dataclass
from typing import List, Any


OPTION_LIST_MAX = 72
DESCRIPTION_OFFSET = 22


@dataclass
class CommandlineParameter:
    """
    Container for defining options for the argument parser.
    """
    short_opt: str = None
    long_opt: str = None
    metavar: str = None
    choices: List[str] = None
    help: str = None
    type: Any = None
    action: str = None
    required: bool = False
    default: Any = None
    is_help: bool = False  # whether a help parameter, gets skipped when building the parser


def param_to_short(param: CommandlineParameter) -> str:
    """
    Turns a parameter definition into a short string for the command list.

    :param param: the parameter to convert into a string
    :type param: CommandlineParameter
    :return: the generated string
    :rtype: str
    """
    if param.short_opt is not None:
        result = param.short_opt
    else:
        result = param.long_opt

    if param.choices is not None:
        result += " {" + ",".join(param.choices) + "}"
    elif param.metavar is not None:
        result += " " + param.metavar

    if not param.required:
        result = "[" + result + "]"

    return result


def params_to_short(prog: str, params: List[CommandlineParameter], additional: str = None) -> str:
    """
    Turns the executable and the parameters into a short overview string.

    :param prog: the executable
    :type prog: str
    :param params: the list of parameters to process
    :type params: list
    :param additional: additional strings to add, ignored if None or empty
    :type additional: str
    :return: the generated string
    :rtype: str
    """
    result = []
    line = "usage: " + prog
    prefix = " " * len(line)
    for param in params:
        param_short = param_to_short(param)
        if len(line) + 1 + len(param_short) > OPTION_LIST_MAX:
            result.append(line)
            line = prefix
        line += " " + param_short
    if len(line) > 0:
        result.append(line)

    if (additional is not None) and (len(additional) > 0):
        result.append(prefix + additional)

    return "\n".join(result)


def param_to_help(param: CommandlineParameter) -> str:
    """
    Generates a long help description for the parameter.

    :param param: the parameter to turn into a help description
    :type param: CommandlineParameter
    :return: the generate help description
    :rtype: str
    """
    # assemble choices
    if param.choices is not None:
        choices = " {" + ",".join(param.choices) + "}"
    else:
        choices = ""

    result = "  "
    if (param.short_opt is None) and (param.long_opt is None):
        raise Exception("Either short or long option flag needs to be provided: %s" % str(param))
    elif (param.short_opt is not None) and (param.long_opt is not None):
        result += param.short_opt + choices + ", " + param.long_opt + choices
    elif param.short_opt is not None:
        result += param.short_opt + choices
    else:
        result += param.long_opt + choices
    if param.metavar is not None:
        result += " " + param.metavar

    # align help text
    if len(result) <= DESCRIPTION_OFFSET:
        result += " " * DESCRIPTION_OFFSET
        result = result[0:DESCRIPTION_OFFSET]
        result += " " + param.help
    else:
        result += "\n" + " " * DESCRIPTION_OFFSET + " " + param.help

    return result


def param_to_parser(parser: argparse.ArgumentParser, param: CommandlineParameter):
    """
    Adds the parameter to the argument parser.
    Skips parameters that are flagged with "is_help=True", as they are handled separately.

    :param parser: the parser to append
    :type parser: argparse.ArgumentParser
    :param param: the parameter to add
    :type param: CommandlineParameter
    """
    if param.is_help:
        return

    args = []
    if param.short_opt is not None:
        args.append(param.short_opt)
    if param.long_opt is not None:
        args.append(param.long_opt)

    kwargs = {}
    if param.metavar is not None:
        kwargs["metavar"] = param.metavar
    if param.choices is not None:
        kwargs["choices"] = param.choices
    if param.help is not None:
        kwargs["help"] = param.help
    if param.action is not None:
        kwargs["action"] = param.action
    if param.type is not None:
        kwargs["type"] = param.type
    kwargs["required"] = param.required
    kwargs["default"] = param.default

    parser.add_argument(*args, **kwargs)


def params_to_parser(parser: argparse.ArgumentParser, params: List[CommandlineParameter]):
    """
    Adds all the parameters to the parser.
    Skips parameters that are flagged with "is_help=True", as they are handled separately.

    :param parser: the parser to append
    :type parser: argparse.ArgumentParser
    :param params: the parameters to add
    :type params: list
    """
    for param in params:
        param_to_parser(parser, param)
