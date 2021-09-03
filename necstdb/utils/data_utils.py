from typing import Sequence, List, Any

StrLike = (str, bytes)


def flatten_data(data: Sequence[Any]) -> List[Any]:
    flattened = []
    for dat in data:
        if isinstance(dat, Sequence) and (not isinstance(dat, StrLike)):
            flattened.extend(dat)
        else:
            flattened.append(dat)
    return flattened
