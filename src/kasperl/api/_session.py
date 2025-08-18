import logging
from typing import Dict, Any

import seppl


class Session(seppl.Session):
    """
    Session object shared among reader, filter(s), writer.
    """
    storage: Dict[str, Any] = dict()
    """ for storing runtime data used within the pipeline. """

    logger: logging.Logger = logging.getLogger("kasperl")
    """ the global logger, derived frameworks should override this with their own. """
