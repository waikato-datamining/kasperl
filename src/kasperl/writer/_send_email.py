import abc
import argparse
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Union

from dotenv import load_dotenv
from wai.logging import LOGGING_WARNING

from kasperl.api import make_list, StreamWriter
from seppl import AnyData
from seppl.placeholders import InputBasedPlaceholderSupporter, placeholder_list

SMTP_HOST = "SMTP_HOST"
SMTP_PORT = "SMTP_PORT"
SMTP_STARTTLS = "SMTP_STARTTLS"
SMTP_USER = "SMTP_USER"
SMTP_PW = "SMTP_PW"
SMTP_ENVS = [
    SMTP_HOST,
    SMTP_PORT,
    SMTP_STARTTLS,
    SMTP_USER,
    SMTP_PW,
]


class SendEmail(StreamWriter, InputBasedPlaceholderSupporter, abc.ABC):

    def __init__(self, dotenv_path: str = None, email_from: str = None, email_to: Union[str, List[str]] = None,
                 subject: str = None, body: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the writer.

        :param dotenv_path: the path to the .env file to use for initializing the SMTP environment vars
        :type dotenv_path: str
        :param email_from: the FROM address to use in the email
        :type email_from: str
        :param email_to: the TO address(es) to use in the email
        :type email_to: str or list
        :param subject: the SUBJECT for the email
        :type subject: str
        :param body: the BODY for the email
        :type body: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.dotenv_path = dotenv_path
        self.email_from = email_from
        self.email_to = email_to
        self.subject = subject
        self.body = body
        self._dotenv_loaded = False
        self._server = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "send-email"

    def description(self) -> str:
        """
        Returns a description of the writer.

        :return: the description
        :rtype: str
        """
        return "Attaches the incoming file(s) and sends them to the specified email address(es)."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-d", "--dotenv_path", metavar="FILE", type=str, help="The .env file to load the SMTP environment variables form (" + "|".join(SMTP_ENVS) + "); tries to load .env from the current directory if not specified; " + placeholder_list(obj=self), required=False, default=None)
        parser.add_argument("-f", "--email_from", metavar="EMAIL", type=str, help="The email address to use for FROM; placeholders get automatically expanded.", default=None, required=True)
        parser.add_argument("-t", "--email_to", metavar="EMAIL", type=str, help="The email address(es) to send the email TO; placeholders get automatically expanded.", default=None, required=True, nargs="+")
        parser.add_argument("-s", "--subject", metavar="SUBJECT", type=str, help="The SUBJECT for the email; placeholders get automatically expanded.", default=None, required=False)
        parser.add_argument("-b", "--body", metavar="TEXT", type=str, help="The email body to use; placeholders get automatically expanded.", default=None, required=False)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.dotenv_path = ns.dotenv_path
        self.email_from = ns.email_from
        self.email_to = ns.email_to
        self.subject = ns.subject
        self.body = ns.body

    def initialize(self):
        """
        Initializes the processing, e.g., for opening files or databases.
        """
        super().initialize()
        if self.email_from is None:
            raise Exception("No FROM address specified!")
        if self.email_to is None:
            raise Exception("At least one TO address must be specified!")
        if self.subject is None:
            self.subject = ""
        if self.body is None:
            self.body = ""
        self._dotenv_loaded = False
        self._server = None

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [AnyData]

    def _attach_data(self, message: MIMEMultipart, data: bytes, name: str,
                     mime_main: str = "application", mime_sub: str = "octet-stream"):
        """
        Attaches the data to the message.

        :param message: the message to attach to
        :type message: MIMEMultipart
        :param data: the data to attach
        :type data: bytes
        :param name: the name to use for the attachment
        :type name: str
        :param mime_main: the main mime type
        :type mime_main: str
        :param mime_sub: the sub mime type
        :type mime_sub: str
        """
        # create attachment
        part = MIMEBase(mime_main, mime_sub)
        part.set_payload(data)
        encoders.encode_base64(part)

        # add header
        part.add_header(
            "Content-Disposition",
            "attachment; filename=%s" % name
        )

        # attach
        message.attach(part)
        self.logger().info("Attached: %s" % name)

    def _attach_file(self, message: MIMEMultipart, path: str):
        """
        Attaches the file to the message.

        :param message: the message to attach to
        :type message: MIMEMultipart
        :param path: the file to attach
        :type path: str
        """
        # create attachment
        with open(path, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
        encoders.encode_base64(part)

        # add header
        part.add_header(
            "Content-Disposition",
            "attachment; filename=%s" % os.path.basename(path)
        )

        # attach
        message.attach(part)
        self.logger().info("Attached: %s" % path)

    @abc.abstractmethod
    def _attach_item(self, message: MIMEMultipart, item) -> bool:
        """
        Attaches the domain-specific item to the message.

        :param message: the message to attach to
        :type message: MIMEMultipart
        :param item: the item to attach
        :return: whether data type has handled
        :rtype: bool
        """
        raise NotImplementedError()

    def _attach_items(self, message: MIMEMultipart, items: List):
        """
        Attaches the items to the message.
        If an element is another list, calls itself with that list again to iterate that list.
        If an element is a string, then it gets attached as file (placeholders get automatically expanded).
        For anything else, the _attach_item method gets called.

        :param message: the message to attach to
        :type message: MIMEMultipart
        :param items: the list of items to attach
        """
        for item in items:
            if isinstance(item, list):
                self._attach_items(message, item)
            elif isinstance(item, str):
                path = self.session.expand_placeholders(item)
                self._attach_file(message, path)
            else:
                if not self._attach_item(message, item):
                    self.logger().warning("Unhandled data type: %s" % str(type(item)))

    def write_stream(self, data):
        """
        Saves the data one by one.

        :param data: the data to write (single record or iterable of records)
        """
        # initialize environment variables
        if not self._dotenv_loaded:
            self._dotenv_loaded = True
            if self.dotenv_path is None:
                path = self.session.expand_placeholders("{CWD}/.env")
                self.logger().info("Loading .env from current dir: %s" % path)
                load_dotenv(path)
            else:
                path = self.session.expand_placeholders(self.dotenv_path)
                self.logger().info("Loading .env: %s" % path)
                load_dotenv(dotenv_path=path)

        try:
            # connect
            if self._server is None:
                self._server = smtplib.SMTP(os.getenv(SMTP_HOST), int(os.getenv(SMTP_PORT)))
                if os.getenv(SMTP_STARTTLS).lower() == "true":
                    self._server.starttls()
                self.logger().info("Logging into SMTP server...")
                self._server.login(os.getenv(SMTP_USER), os.getenv(SMTP_PW))

            # assemble email
            message = MIMEMultipart()
            message["From"] = self.session.expand_placeholders(self.email_from)
            message["To"] = ", ".join(self.session.expand_placeholders(self.email_to))
            message["Subject"] = self.session.expand_placeholders(self.subject)
            message.attach(MIMEText(self.session.expand_placeholders(self.body), "plain"))
            self._attach_items(message, make_list(data))
            self._server.send_message(message)
            self.logger().info("Email sent successfully!")
        except Exception:
            self.logger().error("Failed to send email!", exc_info=True)

    def finalize(self):
        """
        Finishes the processing, e.g., for closing files or databases.
        """
        super().finalize()
        if self._server is not None:
            try:
                self.logger().info("Logging off from SMTP server...")
                self._server.quit()
            except:
                self._server = None
