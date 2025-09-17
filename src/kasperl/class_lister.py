from typing import List, Dict


def list_classes() -> Dict[str, List[str]]:
    return {
        "seppl.io.Reader": [
            "kasperl.reader",
        ],
        "seppl.io.Filter": [
            "kasperl.filter",
        ],
        "seppl.io.Writer": [
            "kasperl.writer",
        ],
        "kasperl.api.Generator": [
            "kasperl.generator",
        ],
    }
