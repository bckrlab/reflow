# import logging
# import random
# from typing import Hashable

# from joblib import Parallel, delayed
# from ..utils_base import PathPatterns, ensure_tuple
# from ..cache.dict_cache_dag import DictCache
# from .utils import dag_to_cache_tree
# from ..recipe import Recipe
# from ..cache.base_dag import Cache
# from .base import CachedExecutor


# logger = logging.getLogger(__name__)
    

# class BreadthFirstExecutor(CachedExecutor):
#     """Executes a recipe in a breadth-first manner.
#     This makes sure that before moving to the next step, all branches of the previous 
#     step have been executed. This makes sure that they are available 
#     in the cache (if enabled) and are not executed again.
#     This is, for example, useful for time consuming preprocessing steps.

#     Notes
#     -----
#     In distributed settings, this executor does not support communication. 
#     Thus, it may still execute the same step multiple times across nodes.

#     **Important**: This executor is still experimental.
#     """

#     def __init__(
#             self, 
#             cache: Cache = None,
#             n_jobs:int|None=None,
#             n_jobs_shuffle:bool=True) -> None:
#         """

#         Parameters
#         ----------
#         cache : Cache, optional
#             The cache to use for storing results, by default None
#         n_jobs : int | None, optional
#             The number of jobs to use for parallelization based on `joblib`, by default None
#         n_jobs_shuffle : bool, optional
#             Whether to shuffle the execution order of jobs.
#             This helps to ensure that the execution paths are mixed well so that caching
#              is more likely to benefit future run. By default True
#         """

#         super().__init__(cache)
#         self.n_jobs = n_jobs
#         self.n_jobs_shuffle = n_jobs_shuffle


#     def _execute(
#             self,
#             recipe:Recipe,
#             input:tuple,
#             last_step: str=None,
#             parallelized_steps: list[str]|str|None=None,
#             execute_include:PathPatterns|None=None,
#             execute_exclude:PathPatterns|None=None,
#             cache_include:PathPatterns|None=None,
#             cache_exclude:PathPatterns|None=None) -> list[tuple]:
#         """Executes a recipe in a breadth-first manner.

#         Parameters
#         ----------
#         recipe : Recipe
#             The recipe to execute
#         input : tuple
#             The input to the first step
#         last_step : str, optional
#             The last step to execute. 
#             If None, all steps are executed.
#             By default None
#         parallelized_steps : list[str] | str | None, optional
#             The steps to parallelize.
#             If str, only the given step is parallelized.
#             If None, only the last step is parallelized.
#             By default None
#         execute_include : PathPatterns|None, optional
#             Paths to include for execution, by default None
#         execute_exclude : PathPatterns|None, optional
#             Paths to exclude for execution (applied after include), by default None
#         cache_include : PathPatterns|None, optional
#             Paths to include for caching, by default None
#         cache_exclude : PathPatterns|None, optional
#             Paths to exclude for caching (applied after include), by default None

#         Returns
#         -------
#         Iterable[Tuple[list[Tuple[str, Hashable]], Any]]
#             An iterable of tuples of the form (path, result)
#         """

#         step_names = recipe.list_steps_from()
#         if parallelized_steps is None:
#             # by default we parallelize the last step only
#             # TODO: should be parallelize all by default?
#             parallelized_steps = [step_names[-1]]
#         elif type(parallelized_steps) is str:
#             parallelized_steps = [parallelized_steps]
#         assert all([s in step_names for s in parallelized_steps])

#         execution_lists, result_paths = self._derive_execution_lists(
#             recipe, 
#             last_step=last_step,
#             execute_include=execute_include,
#             execute_exclude=execute_exclude)

#         layer_results_dict = DictCache()
#         for i_layer, execution_list in enumerate(execution_lists):

#             # function for executing in parallel
#             def execute_branch(path:list[(str, str)], input):

#                 logger.debug(f"Executing: {path}")
                
#                 # get input (e.g., load from cache)
#                 if input is None:
#                     input = self._cache_get(
#                         path[:-1], 
#                         default=None,
#                         include=cache_include, 
#                         exclude=cache_exclude)
                
#                 # calculate step
#                 step_name, branch_name = path[-1]
#                 func = recipe.steps[step_name]["branches"][branch_name]["func"]
#                 result = func(*ensure_tuple(input))

#                 # cache result
#                 # TODO: if cache is in-memory, accessing cache here MAY duplicate memory
#                 self._cache_set(
#                     path, result, include=cache_include, exclude=cache_exclude)

#                 return result
            
#             # shuffle paths for execution to prevent 
#             # overlapping executions across workers
#             if self.n_jobs is not None and self.n_jobs_shuffle:
#                 random.shuffle(execution_list)

#             # only parallelize requested steps
#             step_name = step_names[i_layer]
#             if step_name in parallelized_steps:
#                 n_jobs = self.n_jobs
#             else:
#                 n_jobs = None

#             # run in parallel
#             # TODO: we could potentially save everything in a out-of-memory cache
#             #   and access it from there, instead of keeping it in `layer_results_dict`
#             #   in order to keep results from clogging up the memory
#             layer_results = Parallel(n_jobs=n_jobs)(
#                 delayed(execute_branch)(
#                     path, 
#                     layer_results_dict.get(
#                         path[:-1], 
#                         default=None if i_layer > 0 else input)) 
#                 for path in execution_list)
            
#             # save layer results for next layer
#             layer_results_dict.clear()
#             for path, result in zip(execution_list, layer_results):
#                 layer_results_dict.set(path, result)

#         # parse results
#         results = [
#             (
#                 p, 
#                 layer_results_dict.get(p) 
#                 if p in layer_results_dict 
#                 else self._cache_get(
#                     p, 
#                     default=None, 
#                     include=cache_include, 
#                     exclude=cache_exclude)) 
#             for p in result_paths]
        
#         return results


#     def _derive_execution_lists(
#             self, 
#             recipe:Recipe,
#             last_step: str=None,
#             return_result_paths:bool=True,
#             execute_include:PathPatterns|None=None,
#             execute_exclude:PathPatterns|None=None
#         ) -> list[list[tuple[str, Hashable]]]\
#             |tuple[list[list[tuple[str, Hashable]]], list[list[tuple[str, Hashable]]]]:
#         """Derives the execution lists for a recipe that is used by the executor.

#         Parameters
#         ----------
#         recipe : Recipe
#             The recipe to derive the execution lists for
#         last_step : str, optional
#             The last step to execute.
#         return_result_paths : bool, optional
#             Whether to return the result paths, by default True
#         execute_include : PathPatterns, optional
#             Paths to include for execution, by default None
#         execute_exclude : PathPatterns, optional
#             Paths to exclude for execution (applied after include), by default None

#         Returns
#         -------
#         list[list[tuple[str, Hashable]]] | tuple[list[list[tuple[str, Hashable]]], list[list[tuple[str, Hashable]]]]
#             The execution lists for each layer or the execution lists and the result paths
#         """
        
#         n_steps = len(recipe.steps)
        
#         # derive tree
#         dag = recipe.dag(
#             last_step=last_step,
#             include=execute_include,
#             exclude=execute_exclude)
#         tree = dag_to_cache_tree(dag, self.cache)

#         root_node_id = [
#             n for n, source in tree.nodes(data="source") 
#             if source == Recipe.DAG_ROOT][0]

#         # derive execution lists by layer
#         execution_lists = []
#         current_layer = [([], root_node_id)]
#         for _ in range(n_steps):

#             # step_name = list(recipe.steps.keys())[i_step]

#             # derive next layer
#             next_layer = [
#                 (path + [tree.nodes[child_id]["source"]], child_id)
#                 for path, parent_id in current_layer
#                 for _, child_id in tree.out_edges(parent_id)
#                 if parent_id != child_id]
            
#             execution_candidates = [
#                 path 
#                 for path, node_id in next_layer 
#                 if "cached" not in tree.nodes[node_id]
#             ]
#             execution_lists.append(execution_candidates)
#             current_layer = next_layer

#         logger.debug("Execution lists:")
#         for i, execution_list in enumerate(execution_lists):
#             logger.debug(f"- Level {i}: {list(recipe.steps.keys())[i]}")
#             for p in execution_list:
#                 logger.debug(f"  - {p}")

#         if return_result_paths:
#             return execution_lists, [p for p, _ in current_layer]
#         else:
#             return execution_lists
