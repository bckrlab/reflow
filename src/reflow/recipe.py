import functools
import logging
from copy import copy
from typing import Any, Hashable, Tuple

import networkx as nx

logger = logging.getLogger(__name__)

# types
Option = Tuple[str, Hashable]
Options = dict[str, Hashable]
Key = Tuple[str, Options]


class Recipe:
    """Recipe class to define a DAG of steps and options.

    Notes
    -----
    In order to make sure that the inputs of steps are consistent
    with its parent order (given when a step is created), we take advantage of the fact
    that the order of edges (and thus the order of predecessors and successors) is
    consistent with the order they were added.
    Luckily, Python dicts (and thus networkx graphs) maintain order with
    `python>=3.7` and `networkx>=3.0`. Also see:
    https://networkx.org/documentation/stable/release/release_3.0.html#api-changes
    """

    NOOP = "NOOP"
    _DAG_ROOT = "ROOT"

    _LATEST_STEP_ARG_VALUE = "latest step"

    _CONTEXT_KWARG_NAME = "context"

    _OPTION_KWARG_NAME = "option"

    def __init__(self):
        self.steps = nx.DiGraph()
        self.steps.add_node(Recipe._DAG_ROOT)

        self.latest_step = Recipe._DAG_ROOT

        self.on_exists_step = "raise"
        self.on_exists_option = "raise"

        # init steps
        self._init_steps()

    def set_on_exists(self, on_exists: str):
        if on_exists not in ["raise", "default", "update"]:
            raise ValueError(f"Unknown `on_exists` value: {on_exists}")
        self.on_exists_step = on_exists
        self.on_exists_option = on_exists

    def add_step(
        self,
        step: str,
        parents: list[str] | str | None = _LATEST_STEP_ARG_VALUE,
        with_noop: bool = False,
        on_exists: str = "default",
    ) -> dict:
        """Insert a new step.

        Parameters
        ----------
        step : str
            The name of the step
        parents : list[str]|str|None, optional
            If None, the step is inserted as an input node.
            If 'latest step', the step is inserted with the most recently
                added step as parent.
            If str, the step is inserted with the given step as parent.
            if list[str], the step is inserted with the given steps as parents.
            By default None
        with_noop : bool, optional
            Whether to add a NOOP option, by default False
        on_exists : str, optional
            What to do if the step already exists.
            If "raise", an error is raised.
            If "default", the global default is used.
            If "update", the step is updated.

        Returns
        -------
        dict
            Description of the step as a dict with the following structure::
                {
                    "options": {
                        "BRANCH_NAME": {
                            "func": <function>,
                        },
                        ...
                    },
                    "latest_option": None,
                    "default_option": None,
                }
        """

        LP = "Insert step"
        logger.debug(f"{LP}: {step}")

        # check if step exists and update if necessary
        if step in self.steps.nodes:
            if on_exists == "default":
                on_exists = self.on_exists_step

            if on_exists == "raise":
                raise ValueError(f"Step already exists: {step}")

            elif on_exists == "update":
                logger.warn(f"{LP}: Step already exists: {step}")
                self.update_step(step, parents=parents)

            else:
                raise ValueError(f"Unknown `on_exists` value: {on_exists}")
        else:
            logger.debug(f"{LP}: Creating new step: {step}")

        # parse parents and whether step is valid with regard to dependency structure
        if parents is None:
            parents_parsed = [Recipe._DAG_ROOT]
        elif parents == Recipe._LATEST_STEP_ARG_VALUE:
            parents_parsed = [self.latest_step]
        elif type(parents) is str:
            parents_parsed = [parents]
        else:
            parents_parsed = parents
        self._ensure_valid_step(step, parents_parsed)

        # initialize step
        step_dict = {
            "options": dict(),
            "default_option": None,
            "latest_option": None,
        }

        # add new step
        self.steps.add_node(step, **step_dict)
        for parent in parents_parsed:
            self.steps.add_edge(parent, step)

        # add noop option
        if with_noop:
            self.add_option_noop(step)

        # update latest step
        self.latest_step = step

        return step_dict

    def update_step(
        self, step: str, name: str = None, parents: list[str] | str | None = None
    ):
        # init logging
        LP = f"Update step: {step}"
        logger.debug(f"{LP}")

        if step not in self:
            raise ValueError(f"Step does not exist: {step}")

        if name is not None:
            logger.debug(f"{LP}: Renaming: {step} -> {name}")
            if step in self:
                raise ValueError(f"Step already exists: {name}")
            nx.relabel_nodes(self.steps, {step: name}, copy=False)
            step = name

        if parents is not None:
            # get current in edges / parents
            current_in_edges = self.steps.in_edges(step)
            current_parents = [p for p, _ in current_in_edges]
            logger.debug(f"{LP}: Updating parents: {current_parents} -> {parents}")

            # check step validity
            self._ensure_valid_step(step, parents)

            # remove current parents and add new parents
            self.steps.remove_edges_from(current_in_edges)
            self.steps.add_edges_from([(parent, step) for parent in parents])

        # update latest step
        self.latest_step = step

    def insert_step(
        self,
        step: str,
        parent: str | None = None,
        with_noop: bool = False,
        on_exists: str = "default",
    ):
        # init logging
        LP = f"Insert step: {step}"
        logger.debug(f"{LP}")

        if parent is not None:
            if len(self.steps) > 1 and parent not in self:
                raise ValueError(f"Parent step does not exist: {parent}")
            children = list(self.steps.successors(parent))
        else:
            children = []

        parents_parsed = [parent] if parent is not None else None
        self.add_step(
            step, parents=parents_parsed, with_noop=with_noop, on_exists=on_exists
        )

        for child in children:
            self.steps.remove_edge(parent, child)
            self.steps.add_edge(step, child)

        # update latest step
        self.latest_step = step

    def contains_step(self, step: str) -> bool:
        return step != Recipe._DAG_ROOT and step in self.steps

    def delete_step(self, step: str):
        if step not in self.steps:
            logger.warn(f"Step does not exist: {step}")
        else:
            descendants = nx.descendants(self.steps, step)
            self.steps.remove_nodes_from(descendants)
            logger.debug(f"Deleted steps: {step}, {descendants}")

    def options(self, step: str) -> dict[str, Any]:
        if step not in self.steps:
            raise ValueError(f"Step does not exist: {step}")
        else:
            return self.steps.nodes[step]["options"]

    def update_options(
        self,
        step: str,
        options: list[(str, callable)] | str | None = None,  # `reset` or `Recipe.NOOP`
        latest_option: Hashable = None,
        default_option: Hashable = None,
    ):
        # init logging
        LP = f"Update options: {step}"

        if options is not None:
            # save old options
            current_options = list(self.steps.nodes[step]["options"].keys())

            # reset options
            self.steps.nodes[step]["options"] = dict()

            if type(options) is str:
                if options == "reset":
                    logger.debug(f"{LP}: Reset options: {current_options} -> []")
                    pass
                elif options == Recipe.NOOP:
                    logger.debug(
                        f"{LP}: Reset options: {current_options} -> [{Recipe.NOOP}]"
                    )
                    self.add_option_noop(step)
                else:
                    raise ValueError(f"Unknown options value: {options}")
            else:
                for option, func in options:
                    if option == Recipe.NOOP:
                        self.add_option_noop(step)
                    else:
                        self.add_option(func, step=step, option=option)

        current_options = self.steps.nodes[step]["options"]

        if self._not_none_and_existing_option(
            default_option, current_options, "default"
        ):
            logger.debug(f"{LP}: Setting default option: {default_option}")
            self.steps.nodes[step]["default_option"] = default_option

        if self._not_none_and_existing_option(latest_option, current_options, "latest"):
            logger.debug(f"{LP}: Setting latest option: {latest_option}")
            self.steps.nodes[step]["latest_option"] = latest_option

    def add_option(
        self,
        func: callable,
        step: str = None,
        option: Hashable = None,
        default: bool = False,
        on_exists: str = "default",  # 'raise' | 'default' | 'update'
        on_missing_step: str = "add",  # 'raise' | 'add'
    ):
        # derive step and option name
        step_parsed, option_parsed = derive_step_and_option_name(
            func, step, option, self.latest_step
        )

        # init logging
        LP = "Add option"
        logger.debug(f"{LP}: {step_parsed}={option_parsed}")

        # get step information or initialize step
        if step_parsed not in self:
            if on_missing_step == "raise":
                raise ValueError(f"Step does not exist: {step_parsed}")

            elif on_missing_step == "add":
                logger.debug(f"{LP}: Step does not exist. Add step: {step_parsed}")
                self.add_step(step=step_parsed, with_noop=False, on_exists="raise")

            else:
                raise ValueError(f"Unknown `on_missing_step` value: {on_missing_step}")

        else:
            logger.debug(f"{LP}: Step exists: {step_parsed}")

        # set option

        step_dict = self.steps.nodes[step_parsed]

        if option_parsed in step_dict["options"] and not self.allow_overwrite:
            if on_exists == "default":
                on_exists = self.on_exists_option

            if on_exists == "raise":  # only happens if step already existed
                raise ValueError(
                    f"Option already exists: {step_parsed}={option_parsed}"
                )

            elif on_exists == "update":
                logger.warn(f"{LP}: Option exists. Overwriting: {option_parsed}")

            else:  # only happens if step already existed
                raise ValueError(f"Unknown `on_exists` value: {on_exists}")

        step_dict["options"][option_parsed] = {
            "func": copy(func),
        }
        step_dict["latest_option"] = option_parsed
        if default:
            step_dict["default_option"] = option_parsed

        # set latest
        self.latest_step = step_parsed

        return step_parsed, option_parsed

    def add_option_noop(self, step: str) -> None:
        def noop(*args, **kwargs):
            return args if len(args) > 2 else args[0]

        self.add_option(noop, step=step, option=Recipe.NOOP)

    def delete_option(self, step: str, option: str):
        # init logging
        LP = "Delete option: {step}={option}"
        logger.debug(f"{LP}")

        if step in self.steps:
            if option in self.steps[step]["options"]:
                del self.steps[step]["options"][option]
            else:
                logger.warn(f"{LP}: Option does not exist: {option}")
        else:
            logger.warn(f"{LP}: Step does not exist: {step}")

    def contains_option(self, step: str, option: str) -> bool:
        return step in self.steps and option in self.steps.nodes[step]["options"]

    def option(
        self,
        step: str = None,
        option: Any = None,
        default: bool = False,
        on_exists: str = "default",
        on_missing_step: str = "add",
    ):
        """Decorator for functions to register as step options automatically.

        Parameters
        ----------
        step : str, optional
            The step name, by default None
            If None, the step name is derived from the function name
            (`def {step}__{option}(): ...`).
        option : Any, optional
            The option name, by default None
            If None, the option name is derived from the function name
            (`def {step}__{option}(): ...`).
        default : bool, optional
            Whether to set the option as default, by default False
        on_exists : str, optional
            What to do if the option already exists, by default 'default'
            If "raise", an error is raised.
            If "default", the global default `on_exists_option` is used.
            If "update", the option is updated.
        on_missing_step : str, optional
            What to do if the step does not exist, by default 'add'
            If "raise", an error is raised.
        """

        def decorator_recipe_step(func):
            step_parsed, option_parsed = self.add_option(
                func,
                step=step,
                option=option,
                default=default,
                on_exists=on_exists,
                on_missing_step=on_missing_step,
            )

            @functools.wraps(func)
            def wrapper_recipe_step(*args, **kwargs):
                # if we have the branch keyword given,
                # switch step functions accordingly
                option_to_execute = kwargs.pop(Recipe._OPTION_KWARG_NAME, None)
                if option_to_execute is None:
                    option_to_execute = option_parsed

                # get branch function to execute
                step_dict = self.steps.nodes[step_parsed]
                func_option = step_dict["options"][option_to_execute]["func"]

                # execute branch function
                value = func_option(*args, **kwargs)
                return value

            return wrapper_recipe_step

        return decorator_recipe_step

    def input_steps(self) -> list[str]:
        return list(self.steps.successors(Recipe._DAG_ROOT))

    def output_steps(self) -> list[str]:
        return [n for n in self.steps.nodes if self.steps.out_degree(n) == 0]

    def latest_options(self) -> Options:
        return {
            step: self.steps.nodes[step]["latest_option"]
            for step in self.steps.nodes
            if step != Recipe._DAG_ROOT
        }

    def __str__(self) -> str:
        tab = "    "

        input_steps = self.input_steps()
        output_steps = self.output_steps()

        layers = list(
            nx.bfs_successors(
                self.steps, Recipe._DAG_ROOT, sort_neighbors=lambda x: sorted(x)
            )
        )

        def format_step(step, node_type):
            parents = self.steps.predecessors(step)
            step_data = self.steps.nodes[step]

            string = ""
            string += f"{2*tab}{step}\n"

            if node_type != "input":
                string += f"{3*tab}parents:\n"
                for parent in parents:
                    string += f"{4*tab}{parent}\n"

            step_data = self.steps.nodes[step]
            options = step_data["options"]
            string += f"{3*tab}options:{'        None' if len(options) == 0 else ''}\n"
            for option in options:
                string += f"{4*tab}{option}\n"

            string += f"{3*tab}default_option: {step_data['default_option']}\n"
            string += f"{3*tab}latest_option:  {step_data['latest_option']}\n"

            return string

        string = "Recipe\n"

        string += f"{tab}INPUT\n"
        for step in input_steps:
            string += format_step(step, "input")

        string += f"{tab}STEPS\n"
        for n, steps in layers:
            for step in steps:
                if (
                    step != Recipe._DAG_ROOT
                    and step not in input_steps
                    and step not in output_steps
                ):
                    string += format_step(step, "step")

        string += f"{tab}OUTPUT\n"
        for step in output_steps:
            string += format_step(step, "output")

        return string

    def __repr__(self):
        return self.__str__()

    def __contains__(self, step: str):
        return self.contains_step(step)

    def _init_steps(self):
        """
        For subclasses of recipe to implement in order to pre-define steps.
        This is helpful to share recipes as self-contained classes rather than scripts.

        For example:
        ```python
        class MyRecipe(rp.Recipe):
            def init_steps(self):

                @self.step()
                def test1():
                    return "Blubb 1"

                @self.step()
                def test2 (x):
                    return x + " > Blubb 2"
        ```
        """

        pass

    def _ensure_valid_step(self, step: str, parents=list[str] | None) -> bool:
        if step == Recipe._LATEST_STEP_ARG_VALUE or step == Recipe._DAG_ROOT:
            raise ValueError(f"Step name not allowed: {step}")

        if parents is None:
            return

        if step in parents:
            raise ValueError(f"Cannot insert step after itself: {step}")

        for p in parents:
            if p not in self.steps:
                raise ValueError(f"Parent step does not exist: {p}")

        if step in self:
            # check for potential circles
            circle_steps = []
            missing_parents = []
            descendants = nx.descendants(self.steps, step)
            for p in parents:
                if p not in self:
                    missing_parents.append(p)

                if p in descendants:
                    circle_steps.append(p)

            if len(missing_parents) > 0:
                raise ValueError(f"Parent steps do not exist: {missing_parents}")

            if len(circle_steps) > 0:
                raise ValueError(f"Descendant steps can not be parents: {circle_steps}")

    def _not_none_and_existing_option(
        self,
        option: str | None,
        options: dict[str, callable] | list[str] | None,
        option_type: str = None,
    ) -> bool:
        if options is None:
            raise ValueError(
                f"The given {option_type} option is not valid "
                f"if the given options are None: "
                f"{option}"
            )

        if option is not None:
            if option not in options:
                raise ValueError(
                    f"The given {option_type} option does not exist in options: "
                    f"{option} not in {options}"
                )
            else:
                return True
        else:
            return False


def derive_step_and_option_name(func=None, step=None, option=None, default_step=None):
    function_name = func.__name__

    split = function_name.split("___")
    if len(split) == 1:
        step_parsed, option_parsed = split[0], "default"
    elif len(split) == 2:
        step_parsed, option_parsed = split
    else:
        step_parsed, option_parsed = split[0], "___".join(split[1:])

    if step is not None:
        step_parsed = step
    if option is not None:
        option_parsed = option

    if option is None:
        if function_name == "<lambda>":
            option_parsed = "default"

    if step_parsed == "<lambda>":
        if default_step is None:
            raise ValueError(
                f"Please provide a step name for the lambda function: {func}"
            )
        step_parsed = default_step

    return step_parsed, option_parsed
