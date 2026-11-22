from kasperl.api import DataFormatter, NameSupporter


class NameFormatter(DataFormatter):
    """
    Extracts the name from data that implements the kasperl.api.NameSupporter mixin.
    """

    def name(self) -> str:
        """
        Returns the name of the handler, used as sub-command.

        :return: the name
        :rtype: str
        """
        return "df-name"

    def description(self) -> str:
        """
        Returns a description of the handler.

        :return: the description
        :rtype: str
        """
        return "Extracts the name from data that implements the kasperl.api.NameSupporter mixin."

    def format_data(self, data) -> str:
        """
        Turns the data into a formatted string.

        :param data: the data to format
        :return: the generated string
        :rtype: str
        """
        if isinstance(data, NameSupporter):
            return data.get_name()
        else:
            self.logger().warning("Does not implement kasperl.api.NameSupporter, cannot obtain name!")
            return ""
