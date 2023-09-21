from typing import Any
import cloudpickle

from .pickler import Pickler


class CloudPickler(Pickler):
    """Pickler using `pickle` as default"""

    def dump(self, value:Any, path:str) -> None:
        with open(path, "wb") as f:
            cloudpickle.dump(value, f)

    def load(self, path: str) -> Any:
        with open(path, "rb") as f:
            return cloudpickle.load(f)