import os
import pathlib
import secrets
import string
import tempfile
import threading
import time
from typing import Any, Hashable, Iterable, Tuple

import mlflow
from mlflow.entities import Run
from mlflow.store.tracking import SEARCH_MAX_RESULTS_THRESHOLD
from mlflow.tracking import MlflowClient

from .base import Cache, Key, Options
from .peristance.pickler import Pickler
from .peristance.pickler_pickle import DefaultPickler
from .utils import format_key, format_options


class MlflowCache(Cache):
    """A cache that stores paths and items via `MLflow <https://mlflow.org>`_."""

    TAG_SYS_PREFIX = "sys."
    PARAM_OPTION_PREFIX = "option."
    MAX_RESULTS = SEARCH_MAX_RESULTS_THRESHOLD

    def __init__(
        self,
        mlflow_experiment_name="default",
        mlflow_cache_name="default",
        mlflow_client: MlflowClient | str = None,
        mlflow_tag_defaults: dict[str, str] = None,
        pickler: Pickler | str = "pickle",
        artifact_progress_bar: bool = False,
    ) -> None:
        """Initialize the cache.

        Parameters
        ----------
        mlflow_experiment_name : str, optional
            The MLflow experiment name, by default "default"
        mlflow_cache_name : str, optional
            The cache name added as a tag to each MLflow run, by default "default"
        mlflow_client : MlflowClient | str, optional
            The MLflow client.
            If `None` this defaults to using the default `MlflowClient()`
            which stores the data in the local file system
            , by default None
        mlflow_tag_defaults : dict[str,str], optional
            Default tags for each Run stored in MLflow, by default None
        pickler : Pickler | str, optional
            The pickler used to store items, by default 'pickle'
        artifact_progress_bar: bool
            Whether to show a progress bar open loading artifacts, by default False.
            This is done via the MLflow environment
            variable `MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR`.
            If None the environment variable is not set / changed.
        """

        super().__init__()

        # bugfix for MLflow issue:
        self._bugfix_mlflow_9457_ensure_tracking_uri(True)

        # init client
        if mlflow_client is None:
            self.client = MlflowClient()
        elif isinstance(mlflow_client, str):
            self.client = MlflowClient(tracking_uri=mlflow_client)
        else:
            self.client = mlflow_client

        # create experiment if it does not exist
        exp = self.client.get_experiment_by_name(mlflow_experiment_name)
        if exp is None:
            self.client.create_experiment(mlflow_experiment_name)
            exp = self.client.get_experiment_by_name(mlflow_experiment_name)
        self.experiment = exp

        self.mlflfow_cache_name = mlflow_cache_name
        self.mlflow_tag_defaults = mlflow_tag_defaults

        if pickler == "pickle":
            self.pickler = DefaultPickler()
        else:
            self.pickler = pickler

        if artifact_progress_bar is not None:
            os.environ["MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR"] = (
                "True" if artifact_progress_bar else "False"
            )

        # used for a running id to deduplicate cache entries
        # in case timestamps are the same (only happened on Windows, with Python 3.10)
        self.thread_lock = threading.Lock()
        self.counter = 0

    def set(self, step: str, options: Options, item: Any, cleanup: bool = True) -> None:
        timestamp = time.time_ns()
        seed = self._new_seed()

        # used for a running id to deduplicate cache entries
        # in case timestamps are the same (only happened on Windows, with Python 3.10)
        with self.thread_lock:
            counter = self.counter
            self.counter = self.counter + 1 % 1000

        tags = dict()
        tags[self._tag("cache_name")] = self.mlflfow_cache_name
        tags[self._tag("timestamp")] = str(timestamp)
        tags[self._tag("counter")] = str(counter)
        tags[self._tag("seed")] = seed

        # default tags
        if self.mlflow_tag_defaults is not None:
            for tag, value in self.mlflow_tag_defaults:
                tags[tag] = value

        # path
        formatted_key = format_key(step, options, include_noop=False)
        formatted_options = format_options(options, include_noop=False)

        tags[self._tag("key")] = formatted_key
        tags[self._tag("step")] = step
        tags[self._tag("option")] = options[step]
        tags[self._tag("options")] = formatted_options

        run: Run = self.client.create_run(
            self.experiment.experiment_id,
            # TODO: maybe the length of the name is an issue
            # if the corresponding database field is limited
            run_name=f"{step}={options[step]}" f"___ts-{timestamp}" f"___seed-{seed}",
            tags=tags,
        )

        # path params
        for step_name, option_name in options.items():
            self.client.log_param(
                run.info.run_id,
                f"{MlflowCache.PARAM_OPTION_PREFIX}{step_name}",
                option_name,
            )
        self.client.log_param(run.info.run_id, "step", step)
        self.client.log_param(run.info.run_id, "option", options[step])

        # log artifact
        self._log_artifact(run.info.run_id, (step, options), "key")
        self._log_artifact(run.info.run_id, item, "item")

        # finish run
        self.client.set_terminated(run.info.run_id, status="FINISHED")

        # remove old runs
        if cleanup:
            # TODO: may not be working!?
            runs = self._get_runs(step, options)
            self._remove_old_runs(runs)

    def get(self, step: str, options: Options, default=None) -> Any:
        runs = self._get_runs(step, options)
        if len(runs) == 0:
            return default

        most_recent_run = self._most_recent_run(runs)
        item = self._load_artifact(most_recent_run.info.run_id, "item")
        return item

    def delete(self, step: str, options: Options) -> None:
        runs = self._get_runs(step, options)
        for run in runs:
            self.client.delete_run(run.info.run_id)

    def delete_all(self, step: str, option: Hashable = None) -> None:
        runs = self._get_runs_for_step(step, option)
        for run in runs:
            self.client.delete_run(run.info.run_id)

    def items(self) -> Iterable[Tuple[Key, Any]]:
        filter_list = [
            self._tag_filter("cache_name", self.mlflfow_cache_name),
            "attributes.status = 'FINISHED'",
        ]

        runs = self.client.search_runs(
            self.experiment.experiment_id,
            filter_string=" AND ".join(filter_list),
            max_results=MlflowCache.MAX_RESULTS,
        )

        # this might not be necessary?
        runs = list(
            reversed(list(sorted(runs, key=lambda r: r.data.tags[self._tag("key")])))
        )

        last_key_string = None
        for run in runs:
            key_string = run.data.tags[self._tag("key")]
            if key_string != last_key_string:
                key = self._load_artifact(run.info.run_id, "key")
                item = self._load_artifact(run.info.run_id, "item")
                last_key_string = key_string
                yield key, item

        return runs

    def keys(self) -> Iterable[list[Tuple[str, Hashable]]]:
        filter_list = [
            self._tag_filter("cache_name", self.mlflfow_cache_name),
            "attributes.status = 'FINISHED'",
        ]

        runs = self.client.search_runs(
            self.experiment.experiment_id,
            filter_string=" AND ".join(filter_list),
            max_results=MlflowCache.MAX_RESULTS,
        )

        # this might not be necessary?
        runs = list(
            sorted(runs, key=lambda r: -int(r.data.tags[self._tag("timestamp")]))
        )

        last_key_string = None
        for run in runs:
            key_string = run.data.tags[self._tag("key")]
            if key_string != last_key_string:
                key = self._load_artifact(run.info.run_id, "key")
                last_key_string = key_string
                yield key

        return runs

    def clear(self) -> None:
        while len(runs := self.client.search_runs(self.experiment.experiment_id)) > 0:
            for run in runs:
                self.client.delete_run(run.info.run_id)

    def contains(self, step: str, options: Options) -> bool:
        return len(self._get_runs(step, options)) > 0

    def _tag(self, tag: str) -> str:
        """Format a tag.

        Parameters
        ----------
        tag : str
            The tag

        Returns
        -------
        str
            The formatted tag
        """
        return f"{MlflowCache.TAG_SYS_PREFIX}{tag}"

    def _tag_filter(self, tag: str, value: str) -> str:
        """Create a filter string for a tag used in an MLflow query for Runs.

        Parameters
        ----------
        tag : str
            The tag
        value : str
            The tag's value

        Returns
        -------
        str
            The filter string
        """
        return f"tag.`{self._tag(tag)}` = '{value}'"

    def _remove_old_runs(self, runs: list[Run]) -> None:
        """Remove old runs for a path that have been superseded by more recent items.

        Parameters
        ----------
        runs : list[Run], optional
            The runs to clean (remove all but the most recent one),
            by default None
        """

        most_recent_run = self._most_recent_run(runs)
        for run in runs:
            if run.info.run_id != most_recent_run.info.run_id:
                self.client.delete_run(run.info.run_id)

    def _most_recent_run(self, runs: list[Run]) -> Run:
        """Get the most recent run from a list of runs
        based on the timestamp tag (and the seed
        in case the same timestamp appears multiple times).

        Parameters
        ----------
        runs : list[Run]
            The runs

        Returns
        -------
        Run
            The most recent run
        """
        values = [
            (r, (r.data.tags[self._tag("timestamp")], r.data.tags[self._tag("seed")]))
            for r in runs
        ]
        most_recent_run = list(sorted(values, key=lambda k: k[1], reverse=True))[0][0]
        return most_recent_run

    def _get_runs(self, step: str, options: Options) -> list[Run]:
        """Get all runs for a given step and options.

        Parameters
        ----------
        step : str
            The step
        options: Options
            The options

        Returns
        -------
        list[Run]
            The runs
        """

        formatted_key = format_key(step, options, include_noop=False)

        filter_list = [
            self._tag_filter("cache_name", self.mlflfow_cache_name),
            self._tag_filter("key", formatted_key),
            "attributes.status = 'FINISHED'",
        ]

        runs = self.client.search_runs(
            self.experiment.experiment_id,
            filter_string=" AND ".join(filter_list),
            max_results=MlflowCache.MAX_RESULTS,
        )

        return runs

    def _get_runs_for_step(self, step: str, option: Hashable = None) -> list[Run]:
        """Get all runs for a given step (or step and branch).

        Parameters
        ----------
        step : str
            The step
        branch : Hashable, optional
            The branch, if `None` all branches are returned,
            by default None

        Returns
        -------
        list[Run]
            The runs for the step (and branch)
        """

        filter_list = [
            self._tag_filter("cache_name", self.mlflfow_cache_name),
            "attributes.status = 'FINISHED'",
            self._tag_filter("step", step),
        ]
        if option is not None:
            filter_list.append(self._tag_filter("option", option))

        runs = self.client.search_runs(
            self.experiment.experiment_id,
            filter_string=" AND ".join(filter_list),
            max_results=MlflowCache.MAX_RESULTS,
        )

        return runs

    def _load_artifact(self, run_id: str, artifact_name: str) -> Any:
        """Load an artifact from a run based on its id.

        Parameters
        ----------
        run_id : str
            The run id
        artifact_name : str
            The artifact name

        Returns
        -------
        Any
            The loaded artifact
        """

        self._bugfix_mlflow_9457_ensure_tracking_uri()

        with tempfile.TemporaryDirectory() as tmpdirname:
            self.client.download_artifacts(
                run_id, f"{artifact_name}.pickle", dst_path=tmpdirname
            )
            file_path = pathlib.Path(tmpdirname) / f"{artifact_name}.pickle"
            result = self.pickler.load(file_path)
            return result

    def _log_artifact(self, run_id: str, item: Any, artifact_name: str):
        """Log an artifact to a run based on its id.

        Parameters
        ----------
        run_id : str
            The run id
        item : Any
            The item to log
        artifact_name : str
            The artifact name
        """

        self._bugfix_mlflow_9457_ensure_tracking_uri()

        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = pathlib.Path(tmpdirname) / f"{artifact_name}.pickle"
            self.pickler.dump(item, file_path)
            self.client.log_artifact(run_id, file_path)

    def _new_seed(self):
        # source: https://flexiple.com/python/generate-random-string-python/
        return "".join(
            secrets.choice(string.ascii_uppercase + string.ascii_lowercase)
            for i in range(7)
        )

    def _bugfix_mlflow_9457_ensure_tracking_uri(self, reset: bool = False):
        # ensure that tracking uri is set correctly
        # see MLflow issue: https://github.com/mlflow/mlflow/issues/9457
        if reset:
            uri = None
        else:
            uri = self.client.tracking_uri
        mlflow.set_tracking_uri(uri)
