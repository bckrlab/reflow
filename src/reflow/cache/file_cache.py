import hashlib
import pathlib
import pickle
import re
import secrets
import string
import time
from typing import Any, Hashable, Iterable, List, Tuple

from .base import Cache, Options
from .utils import format_options


class FileCache(Cache):
    """A cache that stores items in files.

    Internally the items are stored with a timestamp and a random seed in the filename
    in order to prevent multiple processes to write to the same file.
    While writing to the file, a temporary file is used.
    The get method always returns the item with the latest timestamp.
    This ensures that this cache can be used
    in a multi-process and distributed environments.
    """

    PREFIX_CACHE = "cache___"
    PREFIX_TMP = "tmp___"

    def __init__(self, out_path="./_cache") -> None:
        """Initialize the cache.

        Parameters
        ----------
        out_path : str, optional
            The path to the cache directory, by default "./_cache"
        """
        super().__init__()
        self.out_path = out_path

    def set(self, step: str, options: Options, item: Any, cleanup: bool = True) -> None:
        file_tmp = self._new_file(step, options, tmp=True)
        with open(file_tmp, "wb") as f:
            pickle.dump(((step, options), item), f)
        file_done = self._new_file(step, options)
        file_tmp.rename(file_done)
        if cleanup:
            self._clean_up_old_files(step, options)

    def get(self, step: str, options: Options, default=None) -> Any:
        file = self._get_most_recent_file(step, options)
        if file is not None and file.exists():
            with open(file, "rb") as f:
                key, item = pickle.load(f)
                return item
        else:
            return default

    def contains(self, step: str, options: Options) -> bool:
        return self._get_most_recent_file(step, options) is not None

    def delete(self, step: str, options: Options):
        basename = self._format_basename(step, options)
        out_path = self._out_path()
        for file in out_path.glob(f"{basename}___ts-*___seed-*.pickle"):
            file.unlink(missing_ok=True)

    def delete_all(self, step: str, option: Hashable = None) -> None:
        step_hash = self._hash(step)

        if option is not None:
            option_hash = self._hash(option)

        out_path = self._out_path()
        for f in out_path.iterdir():
            if re.match("cache___.*", f.name):
                file_step_hash, file_option_hash, _ = self._parse_basename(f.name)
                if file_step_hash != step_hash:
                    continue
                elif option is not None and file_option_hash != option_hash:
                    continue
                else:
                    f.unlink()

    def items(self) -> Iterable[Tuple[List[Tuple[str, Hashable]], Any]]:
        out_path = self._out_path()
        files = list(
            sorted(out_path.glob("cache___*___ts-*___seed-*.pickle"), reverse=True)
        )
        last_path = None
        for file in files:
            with open(file, "rb") as f:
                path, item = pickle.load(f)
                if path != last_path:
                    yield path, item
                else:
                    last_path = path

    def clear(self) -> None:
        out_path = self._out_path()
        for file in out_path.iterdir():
            if re.match("cache___.*", file.name):
                if file.is_file():
                    file.unlink()

    def _get_most_recent_file(self, step: str, options: Options) -> pathlib.Path:
        """Get the most recent file for a given path.

        Parameters
        ----------
        step : str
            The step.
        options : Options
            The options.

        Returns
        -------
        pathlib.Path
            The path to the most recent file.
        """
        basename = self._format_basename(step, options)
        out_path = self._out_path()
        files = []
        for file in out_path.glob(f"{basename}___ts-*___seed-*.pickle"):
            files.append(file)
        if len(files) == 0:
            return None
        else:
            newest_file = next(iter(sorted(files, reverse=True)))
            return newest_file

    def _clean_up_old_files(self, step: str, options: Options):
        """Clean up old files for a given path.

        Parameters
        ----------
        step : str
            The step.
        options : Options
            The options.
        """

        basename = self._format_basename(step, options)
        out_path = self._out_path()
        files = []
        for f in out_path.glob(f"{basename}___ts-*___seed-*.pickle"):
            files.append(f)
        for f in list(sorted(files, reverse=True))[1:]:
            f.unlink(missing_ok=True)

    def _new_file(self, step: str, options: Options, tmp: bool = False) -> pathlib.Path:
        """Create a new file for a given path.

        Parameters
        ----------
        step : str
            The step.
        options : Options
            The options.
        tmp : bool, optional
            Whether to create a temporary file (with prefix `tmp___`), by default False

        Returns
        -------
        pathlib.Path
            _description_
        """
        out_path = self._out_path()
        basename = self._format_basename(step, options)

        # source: https://flexiple.com/python/generate-random-string-python/
        seed = "".join(
            secrets.choice(string.ascii_uppercase + string.ascii_lowercase)
            for i in range(7)
        )

        ts = time.time_ns()

        return (
            out_path
            / f"{'tmp___' if tmp else ''}{basename}___ts-{ts}___seed-{seed}.pickle"
        )

    def _out_path(self) -> pathlib.Path:
        """Get the path to the cache directory and create it if it does not exists.

        Returns
        -------
        pathlib.Path
            The path to the cache directory.
        """
        out_path = pathlib.Path(self.out_path)
        out_path.mkdir(parents=True, exist_ok=True)
        return out_path

    def _format_basename(self, step: str, options: Options) -> str:
        """Get the basename of a file for a given path.

        Parameters
        ----------
        step : str
            The step.
        options : Options
            The options.

        Returns
        -------
        str
            The basename.
        """
        formatted_options = format_options(options, include_noop=False)
        options_hash = self._hash(formatted_options)

        return (
            f"cache"
            f"___{self._hash(step)}={self._hash(options[step])}"
            f"___{options_hash}"
        )

    def _parse_basename(self, basename: str) -> (str, str, str):
        """Parse a basename and extract hashes

        Parameters
        ----------
        basename : str
            The basename.

        Returns
        -------
        (str, str)
            The step hash, and the options hash.
        """
        split = basename.split("___")
        step_hash, option_hash = split[1].split("=")
        options_hash = split[2]
        return step_hash, option_hash, options_hash

    def _hash(self, obj: Any) -> str:
        """Hash an object.

        Parameters
        ----------
        obj : Any
            The object to hash.

        Returns
        -------
        str
            The hash.
        """
        return hashlib.md5(str(obj).encode()).hexdigest()
