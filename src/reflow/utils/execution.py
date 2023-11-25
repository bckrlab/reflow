from typing import Any, Tuple

from ..recipe import Recipe


def execution_path_lazy(recipe: Recipe):
    # NOTE: here, output layers refer to leaf nodes
    # this may not be the same as all steps that are returned as output

    sequences = dict({s: [s] for s in recipe.output_steps()})

    # execution layers are layers with steps that can be executed in parallel
    # initialize the first execution layer with all output steps
    # note that this is the last execution layer
    execution_layers = [list(sequences.values())]

    # keeps track of all steps that have already been added to an execution layer
    # output steps already form a last layer
    steps_in_layers = set(sequences.keys())

    # get execution graph and remove root
    dag = recipe.steps.copy()
    dag.remove_node(Recipe._DAG_ROOT)

    # keeps steps that are not ready to be executed
    # because a successor has not been added to an execution layer
    waitlist = []

    # continue while we have not every step has been marked as executed
    while len(steps_in_layers) < len(dag):
        # get all candidates to be executed before the current execution layer
        candidates = set(
            predecessor
            # for all nodes in the current execution layer
            for node in execution_layers[-1]
            # get all predecessors of the node in the original dag
            for predecessor in dag.predecessors(node[-1])
            # we don't need to execute steps that have already been executed
            if predecessor not in steps_in_layers
        )
        # add waitlisted steps to candidates
        candidates.update(waitlist)

        layer = []
        for step in candidates:
            if step in steps_in_layers:
                continue

            # only add the current step to the current layer
            # if all successors have been added to a previous layer
            if all([c in steps_in_layers for c in dag.successors(step)]):
                sequence = [step]

                # add predesessors to the sequence as long as
                # they have only one successor;
                # this represents a simple path that can be merged into a single step
                simple_step = step
                while simple_step is not None:
                    predecessors = list(dag.predecessors(simple_step))
                    simple_step = None
                    if len(predecessors) == 1:
                        predecessor = predecessors[0]
                        successors = list(dag.successors(predecessor))
                        if len(successors) == 1:
                            simple_step = predecessor
                            sequence.append(simple_step)

                # add sequence to the current layer
                layer.append(sequence)
                if step in waitlist:
                    waitlist.remove(step)
            else:
                waitlist.append(step)

        steps_in_layers.update([step for sequence in layer for step in sequence])
        execution_layers.append(sorted(layer))

    return [
        [list(reversed(node)) for node in layer] for layer in reversed(execution_layers)
    ]


def execution_path_eager(recipe: Recipe):
    sequences = dict({s: [s] for s in recipe.input_steps()})
    execution_layers = [list(sequences.values())]
    executed_steps = set(sequences.keys())
    waitlist = []

    dag = recipe.steps

    while len(executed_steps) < len(dag) - 1:
        candidates = set(
            successor
            for node in execution_layers[-1]
            for successor in dag.successors(node[-1])
            if successor not in executed_steps
        )
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


def ensure_tuple(input: Any) -> Tuple[Any, ...]:
    if type(input) is not tuple:
        return (input,)
    else:
        return input


def ensure_dict(input: Any, steps: list[str]) -> dict[str, Tuple[Any, ...]]:
    if type(input) is dict:
        for k in input.keys():
            input[k] = ensure_tuple(input[k])
        return input
    else:
        input_tuple = ensure_tuple(input)
        return {s: input_tuple for s in steps}
