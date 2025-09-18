import os
from typing import List

from kasperl.api import make_list, StreamWriter
from seppl.placeholders import InputBasedPlaceholderSupporter


class DeleteFiles(StreamWriter, InputBasedPlaceholderSupporter):

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "delete-files"

    def description(self) -> str:
        """
        Returns a description of the writer.

        :return: the description
        :rtype: str
        """
        return "Deletes the files associate with the file names it receives. Placeholders in the file names get expanded automatically."

    def accepts(self) -> List:
        """
        Returns the list of classes that are accepted.

        :return: the list of classes
        :rtype: list
        """
        return [str, list]

    def write_stream(self, data):
        """
        Saves the data one by one.

        :param data: the data to write (single record or iterable of records)
        """
        for item in make_list(data):
            path = self.session.expand_placeholders(item)
            if not os.path.exists(path):
                self.logger().warning("File does not exist: %s" % path)
                continue
            if os.path.dirname(path):
                self.logger().warning("Not a file: %s" % path)
                continue
            try:
                self.logger().info("Removing file: %s" % path)
                os.remove(path)
            except:
                self.logger().error("Failed to remove file: %s" % path, exc_info=True)
