import pickle
from typing import Any

from .pickler import Pickler


class DefaultPickler(Pickler):
    """Pickler using `pickle` as default"""

    def dump(self, value:Any, path:str) -> None:
        with open(path, "wb") as f:
            pickle.dump(value, f)

    def load(self, path:str) -> Any:
        with open(path, "rb") as f:
            return pickle.load(f)