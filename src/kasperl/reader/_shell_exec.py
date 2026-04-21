import argparse
import shlex
import subprocess
from typing import List, Iterable

from wai.logging import LOGGING_WARNING

from kasperl.api import Reader
from seppl.variables import variable_list, VariableSupporter


class ShellExec(Reader, VariableSupporter):

    def __init__(self, env_vars: List[str] = None, workdir: str = None,
                 command: str = None, command_file: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param env_vars: the optional environment key=value pairs to use
        :type env_vars: list
        :param workdir: the working directory to use
        :type workdir: str
        :param command: the command to execute
        :type command: str
        :param command_file: the file with the command to execute
        :type command_file: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.env_vars = env_vars
        self.workdir = workdir
        self.command = command
        self.command_file = command_file
        self._command = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "shell-exec"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Executes the external command and forwards the exit code."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-e", "--env_vars", metavar="KEY=VALUE", type=str, help="The environment variable key=value pairs to use for the execution.", required=False, nargs="*")
        parser.add_argument("-w", "--workdir", metavar="DIR", type=str, help="The working directory to use; " + variable_list(obj=self), required=False, default=None)
        parser.add_argument("-c", "--command", metavar="CMD", type=str, help="The command to execute; " + variable_list(obj=self), required=False, default=None)
        parser.add_argument("-C", "--command_file", metavar="PATH", type=str, help="The text file with the command to execute; " + variable_list(obj=self), required=False, default=None)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.env_vars = ns.env_vars
        self.workdir = ns.workdir
        self.command = ns.command
        self.command_file = ns.command_file

    def generates(self) -> List:
        """
        Returns the list of classes that get produced.

        :return: the list of classes
        :rtype: list
        """
        return [str]

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if (self.command is None) and (self.command_file is None):
            raise Exception("Either command or command_file need to be specified!")
        if (self.command is not None) and (self.command_file is not None):
            raise Exception("You can specify either command or command_file, but not both!")
        if self.command is not None:
            self._command = self.command
        if self.command_file is not None:
            with open(self.command_file) as fp:
                lines = fp.readlines()
                lines = [x.strip() for x in lines]
                self._command = " ".join(lines)

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        env_vars = None
        if (self.env_vars is not None) and (len(self.env_vars) > 0):
            env_vars = dict()
            for x in self.env_vars:
                parts = x.split("=")
                if len(parts) == 2:
                    env_vars[parts[0]] = self.session.expand_variables(parts[1])
                else:
                    self.logger().warning("Invalid environment variable format: %s" % x)
            self.logger().info("Using env vars: %s" % str(env_vars))
        workdir = None
        if self.workdir is not None:
            workdir = self.session.expand_variables(self.workdir)
            self.logger().info("Using work dir: %s" % workdir)
        command = shlex.split(self.session.expand_variables(self._command))
        self.logger().info("Executing: %s" % command)
        completed = subprocess.run(command, cwd=workdir, env=env_vars)
        yield str(completed.returncode)

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return True
