from pathlib import Path

import cloudpickle as cp

from .recipe import Recipe

# TODO: this is not great as it is not viable for long term storage ...
# so not sure whether proving this encourages the wrong things
# sharing recipes via python files is better ...


def save(recipe: Recipe, file_path: str | Path, protocol=None):
    if type(file_path) is Path:
        file_path = str(file_path)

    with open(file_path, "wb") as f:
        cp.dump(obj=recipe, file=f, protocol=protocol)


def load(file_path: str | Path):
    if type(file_path) is Path:
        file_path = str(file_path)

    with open(file_path, "rb") as f:
        return cp.load(f)
