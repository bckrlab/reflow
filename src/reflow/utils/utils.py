
# functions (static)

from ast import parse
from copy import deepcopy
from hmac import new
import logging
from typing import Hashable
import numpy as np
import networkx as nx
from .patterns import StepOptionPatternList, parse_step_option_pattern_list

from ..recipe import Recipe, Options, Option


_logger = logging.getLogger(__name__)


def filtered_option_combinations(
        recipe: Recipe,
        output: list[str]|None=None,
        include:StepOptionPatternList|None=None,
        exclude:StepOptionPatternList|None=None,
        on_purge_step:str='raise'
        ):

    recipe = filter_recipe(
        recipe, 
        output=output,
        include=include,
        exclude=exclude, 
        on_purge_step=on_purge_step)
    
    option_combinations = all_option_combinations(recipe)

    filtered_option_combinations = [
        options for options in option_combinations 
        if match_options(options, include=include, exclude=exclude)]
    
    return filtered_option_combinations


def filter_recipe(
        recipe: Recipe,
        output: list[str]|None=None,
        include:StepOptionPatternList|None=None,
        exclude:StepOptionPatternList|None=None,
        on_purge_step:str='raise') -> Recipe:

    _logger.debug(f"Filtering recipe")
    _logger.debug(f"Output: {output}")
    _logger.debug(f"Include: {include}")
    _logger.debug(f"Exclude: {exclude}")


    if on_purge_step not in ['raise', 'warn', 'ignore']:
        raise ValueError(
            f"Invalid value for 'on_purge_step': '{on_purge_step}'")

    include_parsed = parse_step_option_pattern_list(include)
    exclude_parsed = parse_step_option_pattern_list(
        exclude,
        # for list
        on_none_list=False,
        on_empty_list=False,
        # for dict
        on_none_option=False,
        on_missing_step=False,
        # for option pattern
        on_empty_element_list=False,
        on_none_element_pattern=False)

    recipe = deepcopy(recipe)

    if output is None:
        output = recipe.output_steps()

    _logger.debug(f"Output steps: {output}")

    # filter output
    steps = set(output)
    steps.add(Recipe._DAG_ROOT)
    for step in output:
        _logger.debug(f"Step: {step}")
        ancestors = nx.ancestors(recipe.steps, step) 
        _logger.debug(f"Ancestors: {ancestors}")
        steps.update(ancestors)
    redundant_steps = set(recipe.steps.nodes) - steps

    _logger.debug(f"Removed redundant steps: {redundant_steps}")

    recipe.steps.remove_nodes_from(redundant_steps)

    # filter options
    for step in recipe.steps.nodes:


        if step == Recipe._DAG_ROOT:
            continue

        old_options = recipe.steps.nodes[step]['options']
        
        _logger.debug(
            f"Filtering options for step: '{step}={list(old_options.keys())}'")

        new_options = {
            o:d for o, d in old_options.items() 
            if include_parsed(step, o) and not exclude_parsed(step, o)}

        recipe.steps.nodes[step]['options_original'] = old_options
        recipe.steps.nodes[step]['options'] = new_options

        # check whether a step is completely purged and what to do about it
        purge = len(new_options) == 0
        if purge and on_purge_step == 'raise':
            raise ValueError(f"Excluding all options for step: '{step}'")
        elif purge and on_purge_step == 'warn':
            _logger.warning(f"Excluding all options for step: '{step}'")
        elif purge and on_purge_step == 'ignore':
            pass

    # drop steps without options
    # TODO: some sanity checks would be nice here

    steps_to_remove = []
    for step in recipe.steps.nodes:

        if step == Recipe._DAG_ROOT:
            continue

        if len(recipe.steps.nodes[step]['options']) == 0:
            predecessors = recipe.steps.predecessors(step)
            successors = recipe.steps.successors(step)

            new_edges = [(src, dst)
                for src in predecessors
                for dst in successors]

            recipe.steps.add_edges_from(new_edges)
            steps_to_remove.append(step)

    recipe.steps.remove_nodes_from(steps_to_remove)

    return recipe 


def all_option_combinations(recipe: Recipe) -> list[Options]:
    steps = [s for s in recipe.steps.nodes if s != Recipe._DAG_ROOT]
    options = [
        np.array(list(recipe.steps.nodes[s]['options'].keys()))
        for s in steps if s != Recipe._DAG_ROOT]
    combinations = cartesian_product(*options)
    return [
        dict(zip(steps, c))
        for c in combinations]


def cartesian_product(*arrays):
    """Source: https://stackoverflow.com/a/11146645"""
    la = len(arrays)
    arr = np.empty([len(a) for a in arrays] + [la], dtype=object)
    for i, a in enumerate(np.ix_(*arrays)):
        arr[...,i] = a
    return arr.reshape(-1, la)


def match_options(
        options:Options,
        include:StepOptionPatternList|None=None,
        exclude:StepOptionPatternList|None=None):
    
    include_parsed = parse_step_option_pattern_list(
        include,         
        # for list
        on_none_list=True,
        on_empty_list=True,
        # for dict
        on_none_option=True,
        on_missing_step=True,
        # for option pattern
        on_empty_element_list=True,
        on_none_element_pattern=False)
    
    exclude_parsed = parse_step_option_pattern_list(
        exclude,
        # for list
        on_none_list=False,
        on_empty_list=False,
        # for dict
        on_missing_step=False,
        on_none_option=False,
        # for option pattern
        on_empty_element_list=False,
        on_none_element_pattern=False)

    return all(include_parsed(step, option) for step, option in options.items()) \
        and not any(exclude_parsed(step, option) for step, option in options.items())