from dataclasses import asdict, dataclass
from typing import Dict


def to_tuple(value: Dict[str, bytes]):
    """_summary_
    method to transform array from dict to list of tuple.
    @param Dict[str, bytes] value
    :param value:
    """
    return [(k, v) for k, v in value.items()]


@dataclass
class HeaderClass:

    def sto_dict(self):
        """_summary_
        method for transform headerClass values on dict
        @param headerClass dataclass
        """
        _dict = {}
        for value in asdict(self).values():
            _dict.update(value)
        return _dict

