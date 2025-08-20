import copy
import importlib
import inspect
import os
from typing import Optional, Union, List, Callable, Any


def strip_suffix(path: str, suffix: str) -> str:
    """
    Removes the suffix from the file, if possible.

    :param path: the filename to process
    :type path: str
    :param suffix: the suffix to remove (including extension); ignored if None or ""
    :type suffix: str
    :return: the (potentially) updated filename
    :rtype: str
    """
    if suffix is not None:
        if len(suffix) == 0:
            suffix = None
    if suffix is not None:
        if path.endswith(suffix):
            return path[0:-len(suffix)]
    return path


def locate_file(path: str, ext: Union[str, List[str]], rel_path: str = None, suffix: str = None) -> List[str]:
    """
    Tries to locate the associate files for the given path by replacing its extension by the provided ones.

    :param path: the base path to use
    :type path: str
    :param ext: the extension(s) to look for (incl dot)
    :type ext: str or list
    :param suffix: the suffix to strip from the files, ignored if None or ""
    :type suffix: str
    :param rel_path: the relative path to the annotation to use for looking for associated files, ignored if None
    :type rel_path: str
    :return: the located files
    :rtype: list
    """
    result = []
    if rel_path is not None:
        parent_path = os.path.dirname(path)
        name = os.path.basename(path)
        path = os.path.join(parent_path, rel_path, name)
    path = strip_suffix(path, suffix)
    noext = os.path.splitext(path)[0]
    for current in ext:
        path = noext + current
        if os.path.exists(path):
            result.append(path)
    return result


def load_function(function: str) -> Callable:
    """
    Parses the function definition and returns the function.
    The default format is "module_name:function_name".
    Raises exceptions if wrong format, missing or not an actual function.

    :param function: the function definition to parse
    :type function: str
    :return: the parsed function
    """
    if ":" not in function:
        raise Exception("Expected format 'module_name:function_name' but got: %s" % function)
    else:
        module_name, func_name = function.split(":")

    try:
        module = importlib.import_module(module_name)
    except:
        raise Exception("Failed to import class lister module: %s" % module_name)

    if hasattr(module, func_name):
        func = getattr(module, func_name)
        if inspect.isfunction(func):
            return func
        else:
            raise Exception("Not an actual function: %s" % function)
    else:
        raise Exception("Function '%s' not found in module '%s'!" % (func_name, module_name))


def safe_deepcopy(obj: Optional[Any]) -> Optional[Any]:
    """
    Creates a deep copy of the object. Skips None objects.

    :param obj: the object to copy, can be None
    :return: the copy or None
    """
    if obj is None:
        return None
    else:
        return copy.deepcopy(obj)


def annotation_to_name(ann: str, ext: str = ".jpg") -> str:
    """
    Generates a name from the annotation file name.

    :param ann: the annotation to create the name from
    :type ann: str
    :param ext: the extension to use for the name (incl dot)
    :type ext: str
    :return: the generated name
    :rtype: str
    """
    result = os.path.basename(ann)
    result = os.path.splitext(result)[0] + ext
    return result
