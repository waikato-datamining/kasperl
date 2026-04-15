from kasperl.api import DataFormatter


class SimpleStringFormatter(DataFormatter):
    """
    Just applies the str(...) method to the data to generate the string.
    """

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "df-simple-string"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Just applies the str(...) method to the data to generate the string."

    def format_data(self, data) -> str:
        """
        Turns the data into a formatted string.

        :param data: the data to format
        :return: the generated string
        :rtype: str
        """
        return str(data)
