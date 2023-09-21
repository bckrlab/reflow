from typing import Any, Tuple
from ..recipe import Recipe


def execution_path_lazy(recipe: Recipe):
    
    sequences = dict({s:[s] for s in recipe.output_steps()})
    execution_layers = [list(sequences.values())]
    executed_steps = set(sequences.keys())
    waitlist  = []

    dag = recipe.steps.reverse(copy=True)
    dag.remove_node(Recipe._DAG_ROOT)

    while len(executed_steps) < len(dag):

        candidates = set(
            successor
            for node in execution_layers[-1]
            for successor in dag.successors(node[-1])
            if successor not in executed_steps)
        candidates.update(waitlist)
        
        layer = []
        for step in candidates:

            if step in executed_steps:
                continue

            if all([c in executed_steps for c in dag.predecessors(step)]):
                sequence = [step]
                while step is not None:
                    successors = list(dag.successors(step))
                    step = None
                    if len(successors) == 1:
                        successor = successors[0]
                        predesessors = list(dag.predecessors(successor))
                        if len(predesessors) == 1:
                            step = successor
                            sequence.append(step)

                layer.append(sequence)
                executed_steps.update(sequence)
                if step in waitlist:
                    waitlist.remove(step)
            else:
                waitlist.append(step)

        execution_layers.append(sorted(layer))

    return \
        [
            [
                list(reversed(node)) 
                for node in layer
            ] 
            for layer in reversed(execution_layers)
        ]


def execution_path_eager(recipe: Recipe):
    
    sequences = dict({s:[s] for s in recipe.input_steps()})
    execution_layers = [list(sequences.values())]
    executed_steps = set(sequences.keys())
    waitlist  = []

    dag = recipe.steps

    while len(executed_steps) < len(dag) - 1:

        candidates = set(
            successor
            for node in execution_layers[-1]
            for successor in dag.successors(node[-1])
            if successor not in executed_steps)
        candidates.update(waitlist)
        
        layer = []
        for step in candidates:

            if step in executed_steps:
                continue

            if all([c in executed_steps for c in dag.predecessors(step)]):
                sequence = [step]
                while step is not None:
                    successors = list(dag.successors(step))
                    step = None
                    if len(successors) == 1:
                        successor = successors[0]
                        predesessors = list(dag.predecessors(successor))
                        if len(predesessors) == 1:
                            step = successor
                            sequence.append(step)

                layer.append(sequence)
                executed_steps.update(sequence)
                if step in waitlist:
                    waitlist.remove(step)
            else:
                waitlist.append(step)

        execution_layers.append(sorted(layer))

    return execution_layers


def ensure_tuple(input:Any) -> Tuple[Any, ...]:
    if type(input) is not tuple:
        return (input, )
    else:
        return input
    
def ensure_dict(input:Any, steps:list[str]) -> dict[str, Tuple[Any, ...]]:

    if type(input) is dict:
        for k in input.keys():
            input[k] = ensure_tuple(input[k])
        return input
    else:
        input_tuple = ensure_tuple(input)
        return {s: input_tuple for s in steps}
    