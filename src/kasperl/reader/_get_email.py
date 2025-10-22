import argparse
import email
import imaplib
import os
import re
from time import sleep
from typing import List, Iterable

from email.header import decode_header
from dotenv import load_dotenv
from wai.logging import LOGGING_WARNING

from kasperl.api import Reader
from seppl.io import InfiniteReader
from seppl.placeholders import placeholder_list, PlaceholderSupporter, add_placeholder


IMAP_HOST = "IMAP_HOST"
IMAP_PORT = "IMAP_PORT"
IMAP_USER = "IMAP_USER"
IMAP_PW = "IMAP_PW"
IMAP_ENVS = [
    IMAP_HOST,
    IMAP_PORT,
    IMAP_USER,
    IMAP_PW,
]

DEFAULT_POLL_WAIT = 30.0

DEFAULT_POLL_WAIT_SLOW = 180.0

DEFAULT_POLL_COUNT = 10


class GetEmail(Reader, InfiniteReader, PlaceholderSupporter):

    def __init__(self, dotenv_path: str = None, folder: str = None, only_unseen: bool = None, mark_as_read: bool = None,
                 regexp: str = None, output_dir: str = None, poll_wait: float = None,
                 poll_count: int = None, poll_wait_slow: float = None, max_poll: int = None,
                 from_placeholder: str = None, subject_placeholder: str = None,
                 logger_name: str = None, logging_level: str = LOGGING_WARNING):
        """
        Initializes the reader.

        :param dotenv_path: the path to the .env file to use for initializing the IMAP environment vars
        :type dotenv_path: str
        :param folder: the folder to obtain the emails from, default is INBOX
        :type folder: str
        :param only_unseen: whether to list only unseen emails
        :type only_unseen: bool
        :param mark_as_read: whether to mark the retrieved emails as seen
        :type mark_as_read: bool
        :param regexp: the regular expression that the attachments must match, ignored if None
        :type regexp: str
        :param output_dir: the path of the text file to load
        :type output_dir: str
        :param poll_wait: the seconds to wait between polls
        :type poll_wait: float
        :param poll_count: the number of times to use poll_wait before switching to poll_wait_slow
        :type poll_count: int
        :param poll_wait_slow: the seconds to wait between polls (during downtime)
        :type poll_wait_slow: float
        :param max_poll: the number of times to poll (<= 0 for infinite)
        :type max_poll: int
        :param from_placeholder: the placeholder name for storing the FROM email address under
        :type from_placeholder: str
        :param subject_placeholder: the placeholder name for storing the SUBJECT under
        :type subject_placeholder: str
        :param logger_name: the name to use for the logger
        :type logger_name: str
        :param logging_level: the logging level to use
        :type logging_level: str
        """
        super().__init__(logger_name=logger_name, logging_level=logging_level)
        self.dotenv_path = dotenv_path
        self.folder = folder
        self.only_unseen = only_unseen
        self.mark_as_read = mark_as_read
        self.regexp = regexp
        self.output_dir = output_dir
        self.poll_wait = poll_wait
        self.poll_count = poll_count
        self.poll_wait_slow = poll_wait_slow
        self.max_poll = max_poll
        self.from_placeholder = from_placeholder
        self.subject_placeholder = subject_placeholder
        self._dotenv_loaded = False
        self._server = None
        self._polled = None
        self._empty_poll_count = None
        self._slow_message_displayed = None

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "get-email"

    def description(self) -> str:
        """
        Returns a description of the reader.

        :return: the description
        :rtype: str
        """
        return "Retrieves emails from the specified IMAP folder, saves the attachments " \
               "in the specified folder and forwards the file names of the saved attachments as list. " \
               "If the number of polls without any new messages reaches the 'poll_count' threshold, " \
               "the polling switches from the 'poll_wait' interval to 'poll_wait_slow'. It will " \
               "automatically reset the next time a new message is encountered."

    def _create_argparser(self) -> argparse.ArgumentParser:
        """
        Creates an argument parser. Derived classes need to fill in the options.

        :return: the parser
        :rtype: argparse.ArgumentParser
        """
        parser = super()._create_argparser()
        parser.add_argument("-d", "--dotenv_path", metavar="FILE", type=str, help="The .env file to load the IMAP environment variables form (" + "|".join(IMAP_ENVS) + "); tries to load .env from the current directory if not specified; " + placeholder_list(obj=self), required=False, default=None)
        parser.add_argument("-f", "--folder", metavar="FOLDER", type=str, help="The IMAP folder to obtain emails from.", required=False, default="INBOX")
        parser.add_argument("-u", "--only_unseen", action="store_true", help="Whether to only retrieve unseen/new emails.", required=False)
        parser.add_argument("-R", "--mark_as_read", action="store_true", help="Whether to mark the emails as read after retrieval.", required=False)
        parser.add_argument("-r", "--regexp", metavar="REGEXP", type=str, help="The regular expression that the attachment file names must match.", required=False, default=None)
        parser.add_argument("-o", "--output_dir", metavar="DIR", type=str, help="The directory to store the attachments in; " + placeholder_list(obj=self), required=True)
        parser.add_argument("-w", "--poll_wait", metavar="SECONDS", type=float, help="The poll interval in seconds", required=False, default=DEFAULT_POLL_WAIT)
        parser.add_argument("-W", "--poll_wait_slow", metavar="SECONDS", type=float, help="The poll interval in seconds during slow operation", required=False, default=DEFAULT_POLL_WAIT_SLOW)
        parser.add_argument("-c", "--poll_count", metavar="THRESHOLD", type=int, help="The maximum number of 'empty' polls that are allowed before switching from 'poll_wait' to 'poll_wait_slow'.", required=False, default=DEFAULT_POLL_COUNT)
        parser.add_argument("-m", "--max_poll", metavar="MAX", type=int, help="The maximum number of times to poll the folder; use <= for infinite polling.")
        parser.add_argument("-F", "--from_placeholder", metavar="PLACEHOLDER", type=str, help="The optional placeholder name to store the FROM email address under, without curly brackets.", required=False, default=None)
        parser.add_argument("-S", "--subject_placeholder", metavar="PLACEHOLDER", type=str, help="The optional placeholder name to store the SUBJECT under, without curly brackets.", required=False, default=None)
        return parser

    def _apply_args(self, ns: argparse.Namespace):
        """
        Initializes the object with the arguments of the parsed namespace.

        :param ns: the parsed arguments
        :type ns: argparse.Namespace
        """
        super()._apply_args(ns)
        self.dotenv_path = ns.dotenv_path
        self.folder = ns.folder
        self.only_unseen = ns.only_unseen
        self.mark_as_read = ns.mark_as_read
        self.regexp = ns.regexp
        self.output_dir = ns.output_dir
        self.poll_wait = ns.poll_wait
        self.poll_wait_slow = ns.poll_wait_slow
        self.poll_count = ns.poll_count
        self.max_poll = ns.max_poll
        self.from_placeholder = ns.from_placeholder
        self.subject_placeholder = ns.subject_placeholder
        self._dotenv_loaded = False
        self._server = None

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
        if self.folder is None:
            self.folder = "INBOX"
        if self.only_unseen is None:
            self.only_unseen = False
        if self.mark_as_read is None:
            self.mark_as_read = True
        if self.regexp is None:
            self.regexp = ""
        if self.output_dir is None:
            raise Exception("No output directory specified!")
        if self.poll_wait is None:
            self.poll_wait = DEFAULT_POLL_WAIT
        if self.poll_wait < 0:
            self.logger().warning("Invalid poll wait '%s', falling back to default of '%s'!" % (str(self.poll_wait), str(DEFAULT_POLL_WAIT)))
            self.poll_wait = DEFAULT_POLL_WAIT
        if self.poll_wait_slow is None:
            self.poll_wait_slow = DEFAULT_POLL_WAIT_SLOW
        if self.poll_wait_slow < 0:
            self.logger().warning("Invalid (slow) poll wait '%s', falling back to default of '%s'!" % (str(self.poll_wait_slow), str(DEFAULT_POLL_WAIT_SLOW)))
            self.poll_wait_slow = DEFAULT_POLL_WAIT_SLOW
        if self.poll_wait_slow < self.poll_wait:
            self.logger().warning("Slow poll wait is lower than normal poll wait, adjusting to '%s' for both!" % str(self.poll_wait))
            self.poll_wait_slow = self.poll_wait
        if self.poll_count is None:
            self.poll_count = DEFAULT_POLL_COUNT
        if self.max_poll is None:
            self.max_poll = -1
        self._polled = 0
        self._empty_poll_count = 0
        self._slow_message_displayed = False

    def read(self) -> Iterable:
        """
        Loads the data and returns the items one by one.

        :return: the data
        :rtype: Iterable
        """
        output_dir = self.session.expand_placeholders(self.output_dir)
        if not os.path.exists(output_dir):
            raise Exception("Output directory does not exist: %s" % output_dir)

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

        poll_wait = self.poll_wait
        # slow down polling?
        if self._empty_poll_count > self.poll_count:
            if not self._slow_message_displayed:
                self._slow_message_displayed = True
                self.logger().info("Number of empty polls reached threshold (%s), switching to slow poll (%ss)." % (str(self.poll_count), str(self.poll_wait_slow)))
            poll_wait = self.poll_wait_slow
        # wait before polling?
        if poll_wait > 0:
            self.logger().info("Waiting for %s seconds before polling" % str(poll_wait))
            sleep(poll_wait)

        try:
            # connect
            if self._server is None:
                self.logger().info("Connection to IMAP host: %s" % os.getenv(IMAP_HOST) + ":" + os.getenv(IMAP_PORT))
                self._server = imaplib.IMAP4_SSL(os.getenv(IMAP_HOST), port=int(os.getenv(IMAP_PORT)))
                self.logger().info("Logging in...")
                self._server.login(os.getenv(IMAP_USER), os.getenv(IMAP_PW))

            self._polled += 1

            # select folder
            self.logger().info("Selecting folder: %s" % self.folder)
            self._server.select(mailbox=self.folder, readonly=False)

            # search messages
            self.logger().info("Looking for emails...")
            if self.only_unseen:
                status, messages = self._server.search(None, 'UNSEEN')
            else:
                status, messages = self._server.search(None, 'ALL')

            if status != 'OK':
                self.logger().warning("Failed to search emails: %s" % status)
            else:
                if isinstance(messages[0], bytes):
                    msg_list = messages[0].decode("utf-8")
                else:
                    msg_list = messages[0]
                if len(msg_list) == 0:
                    self.logger().info("No messages found.")
                    self._empty_poll_count += 1
                    return None

                # reset counter
                if self._empty_poll_count > self.poll_count:
                    self.logger().info("Switching back to normal poll wait (%ss)." % str(self.poll_wait))
                self._empty_poll_count = 0
                self._slow_message_displayed = False

                msg_ids = msg_list.split(' ')
                self.logger().info("# of messages found: %d" % len(msg_ids))
                for msg_id in msg_ids:
                    self.logger().info("Fetching message: %s" % msg_id)
                    status, msg_data = self._server.fetch(msg_id, '(RFC822)')
                    files = []
                    from_ = None
                    subject_ = None
                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            subject_, _ = decode_header(msg["Subject"])[0]
                            from_, _ = decode_header(msg["From"])[0]
                            if not msg.is_multipart():
                                self.logger().info("No attachments, skipping!")
                            else:
                                for part in msg.walk():
                                    content_disposition = str(part.get("Content-Disposition"))
                                    if "attachment" in content_disposition:
                                        # download attachment
                                        filename = part.get_filename()
                                        if filename is not None:
                                            # does the file name match the regexp?
                                            if len(self.regexp) > 0:
                                                if not re.match(self.regexp, filename):
                                                    self.logger().info("Skipping attachment: %s" % filename)
                                                    continue
                                            output_file = os.path.join(self.session.expand_placeholders(self.output_dir), filename)
                                            self.logger().info("Saving attachment to: %s" % output_file)
                                            with open(output_file, "wb") as fp:
                                                fp.write(part.get_payload(decode=True))
                                            files.append(output_file)

                                # mark as read?
                                if self.mark_as_read:
                                    self.logger().info("Marking message as read: %s" % msg_id)
                                    status, data = self._server.uid('store', msg_id, '+FLAGS', '(\\Seen)')
                                    if status != 'OK':
                                        self.logger().warning("Failed to mark message as read: %s" % msg_id)

                    # forward file names of downloaded attachments
                    if len(files) > 0:
                        if (self.from_placeholder is not None) and (from_ is not None):
                            self.logger().info("Setting placeholder '%s' to: %s" % (self.from_placeholder, from_))
                            add_placeholder(self.from_placeholder, "from get-email", False, lambda i: from_)
                        if (self.subject_placeholder is not None) and (subject_ is not None):
                            self.logger().info("Setting placeholder '%s' to: %s" % (self.subject_placeholder, subject_))
                            add_placeholder(self.subject_placeholder, "from get-email", False, lambda i: subject_)
                        yield files
        except:
            self.logger().error("Failed to get emails!", exc_info=True)

    def has_finished(self) -> bool:
        """
        Returns whether reading has finished.

        :return: True if finished
        :rtype: bool
        """
        return (self.max_poll > 0) and (self._polled >= self.max_poll)

    def finalize(self):
        """
        Finishes the processing, e.g., for closing files or databases.
        """
        super().finalize()
        if self.max_poll > 0:
            self.logger().info("# of polls: %s" % str(self._polled))
        if self._server is not None:
            try:
                self._server.close()
            except:
                pass
            self._server = None
