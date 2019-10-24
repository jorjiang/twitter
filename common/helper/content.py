import hashlib
from typing import Iterable


def hash_strings(strings: Iterable[str]) -> str:
    return hashlib.sha1(" ".join(strings).encode("utf-8")).hexdigest()
