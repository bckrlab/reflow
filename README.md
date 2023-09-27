# ReFlow

> ⚠️ **In Development**: I am still preparing `ReFlow` for release on `pypi`. You can already play around with it but be aware that things are changing or might not be complete. This includes this README which still needs some work.

`ReFlow` (Recipe Flow) is an approach for
defining and executing a **directed acyclic graph** (DAG)
with **several options for each node**, a so called `Recipe`,
in an efficient, transparent, and easily comparable manner.
This can be useful, e.g., in data science settings
where many different preprocessing variants or subsets of the data need to be analyzed
and it is not clear which analysis steps to use yet.


Install `ReFlow` via pip:

```bash
pip install git+https://github.com/bckrlab/reflow.git#egg=reflow
```

Simple example:


```python
import reflow as rf

# define recipe

recipe = rf.Recipe()

@recipe.option()
def step1___option1(x):
    return x + "_step1=option1"

@recipe.option()
def step1___option2(x):
    return x + "_step1=option2"

@recipe.option()
def step2(x):
    return x + "_step2=default"

# the recipe can execute all possible option combinations ...
all_results = recipe("some input", include="all")

# ... for which it provides a nice visualization
df = rf.results_to_dataframe(all_results, filter_columns=False)
display(df)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th colspan="2" halign="left">options</th>
      <th>results</th>
    </tr>
    <tr>
      <th></th>
      <th>step1</th>
      <th>step2</th>
      <th>step2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>option1</td>
      <td>default</td>
      <td>some input_step1=option1_step2=default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>option2</td>
      <td>default</td>
      <td>some input_step1=option2_step2=default</td>
    </tr>
  </tbody>
</table>
</div>


## Overview


> ⚠️ **Overfitting:** Use `Recipe`s with care in data science settings, as in predictive settings it might promote overfitting when running many different options.

The idea is to define a `Recipe` similar to `scikit-learn`'s pipeline concept,
but allow for DAGs as well as multiple `option`s for each `step`, i.e., node in the DAG.
This can result in many different execution instantiations (all combinations of options).
Developing such recipes rapidly,
running the recipe efficiently,
and being able to examine the results effectively
is the goal of this library.

**Quickstart:** For a quick overview start with the example in [Quickstart](#quickstart).

**Features**: `Recipe` also provides a wealth of features (not exhaustive):

| Feature                                          | Description                                                                                                                             |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| [Recipes are functions]() ||
| [Execution filtering](#execution-filtering) | Execution filtering enables to only execute specific options and/or combinations of options.                                       |
| [Sessions](#sessions)            | Sessions allows for dynamically executing the `Recipe` while it is defined during development (e.g., in a Jupyter environment), and offer flexible execution options for parallel and distributed execution. |
| [Visualization capabilities](#)                  | `Recipe` has inbuilt (currently rudimentary) capabilities to visualize the recipe as a DAG and results as `pandas` DataFrames.           |
| [Parallel and distributed execution](#)          | Recipes can be run in a parallel and distributed fashion (easily extensible by writing custom `Executors`).                             |
| [Flexible step definitions](#)                   | Steps can be defined based on user needs (for loops, parameters, etc.).                                                                 |
| [Easy sharing and combination of recipes](#)     | Recipes can be combined and shared easily. |
| [Recipes are light-weight](#)     | Recipes can be combined and shared easily. |


**Note 1: Grid Search:** Executing all combinations of options is basically equivalent to a grid search across all options (here called `branches`).

**Note 2: Alternatives.** I designed this since I did not find any existing libraries
that allowed a simple, intuitive, and programmatic way of such a flare / grid search, with caching and a nice way of displaying the results.
However, see the section about [potential alternatives](#potential-alternatives) for an overview and comparisons of existing libraries.
Maybe one of them renders `ReFlow` obsolete (let me know if you think so in the issue tracker).

## Quickstart

Install `ReFlow` via pip:

```bash
pip install git+https://github.com/bckrlab/reflow.git#egg=reflow
```

A `Recipe` is essentially a fancy function, with separate steps and multiple options for each step.
The final recipe allows to run and choose from these steps and options using specific keyword arguments.


```python
import reflow as rf

# define recipe

recipe = rf.Recipe()

@recipe.option()
def step1___option1(x):
    return x + "_step1=option1"

@recipe.option()
def step1___option2(x):
    return x + "_step1=option2"

@recipe.option()
def step2(x):
    return x + "_step2=default"

# examine recipe
print(recipe)
```

    Recipe
        INPUT
            step1
                options:
                    option1
                    option2
                default_option: None
                latest_option:  option2
        STEPS
        OUTPUT
            step2
                parents:
                    step1
                options:
                    default
                default_option: None
                latest_option:  default




```python
step1___option1("test", option="option2")
```




    'test_step1=option2'




```python
step1_switch = "option1"

# input
x = "yuqi"

if step1_switch == "option1":
    x = step1___option1(x)
elif step1_switch == "option2":
    x = step1___option2(x)

x = step2(x)
x
```




    'yuqi_step1=option1_step2=default'




```python
recipe("yuqi", include={"step1": "option1"})
```




    'yuqi_step1=option1_step2=default'




```python
# a recipe is a function executing all steps in sequence
# by default the latest branch for each step
result_from_latest_steps = recipe("some input")
display(result_from_latest_steps)
```


    'some input_step1=option2_step2=default'



```python
# the recipe can also execute all possible branch combinations ...
all_results = recipe("some input", include="all")

# ... for which we also provide a nice visualization
df = rf.results_to_dataframe(all_results, filter_columns=False)
display(df)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th colspan="2" halign="left">options</th>
      <th>results</th>
    </tr>
    <tr>
      <th></th>
      <th>step1</th>
      <th>step2</th>
      <th>step2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>option1</td>
      <td>default</td>
      <td>some input_step1=option1_step2=default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>option2</td>
      <td>default</td>
      <td>some input_step1=option2_step2=default</td>
    </tr>
  </tbody>
</table>
</div>



```python
# note that each step is a function and can be used as expected ...
step_result = step1___option1("some input")
print(step_result)

# ... and additionally, steps can now switch functionality with the `branch` keyword
step_result = step1___option1("some input", option="option2")
print(step_result)
```

    some input_step1=option1
    some input_step1=option2


## Features

### Visualization capabilities


```python
# print recipe
recipe
```




    Recipe
        INPUT
            step1
                options:
                    option1
                    option2
                default_option: None
                latest_option:  option2
        STEPS
        OUTPUT
            step2
                parents:
                    step1
                options:
                    default
                default_option: None
                latest_option:  default




```python
# execute recipe ...
recipe_input = "start"
results = recipe(recipe_input, include="all")

# ... and look at results as dataframe
df = rf.results_to_dataframe(results, filter_columns=False)
display(df)

# ... we can skip columns for steps with only one option
rf.results_to_dataframe(results)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th colspan="2" halign="left">options</th>
      <th>results</th>
    </tr>
    <tr>
      <th></th>
      <th>step1</th>
      <th>step2</th>
      <th>step2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>option1</td>
      <td>default</td>
      <td>start_step1=option1_step2=default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>option2</td>
      <td>default</td>
      <td>start_step1=option2_step2=default</td>
    </tr>
  </tbody>
</table>
</div>





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>options</th>
      <th>results</th>
    </tr>
    <tr>
      <th></th>
      <th>step1</th>
      <th>step2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>option1</td>
      <td>start_step1=option1_step2=default</td>
    </tr>
    <tr>
      <th>1</th>
      <td>option2</td>
      <td>start_step1=option2_step2=default</td>
    </tr>
  </tbody>
</table>
</div>



### Execution path filtering

Recipes can blow up pretty quickly as we add more options and steps.
In that case, it might not make sense to execute every single path.
For this, we provide filtering capabilities in the execution step.


```python
# define a simple filter for a specific path
include = {"step1": "option1", "step2": "default"}

# filters supports regex
include = {"step1": "option1", "step2": ".*"}

# filters can consist of multiple and partial paths
include = [{"step1": "option1"}, {"step1": "option2"}]

# filters support functions
include = {"step1": lambda option: option == "option1"}
```


```python
# look at filtered results
recipe_input = "start"
results = recipe(recipe_input, include=include, squeeze=False)
df = rf.results_to_dataframe(results, filter_columns=False)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th colspan="2" halign="left">options</th>
      <th>results</th>
    </tr>
    <tr>
      <th></th>
      <th>step1</th>
      <th>step2</th>
      <th>step2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>option1</td>
      <td>default</td>
      <td>start_step1=option1_step2=default</td>
    </tr>
  </tbody>
</table>
</div>



**Note:** There is also an `exclude` filter which exectus *after* the include filter.

### Interactive mode

Interactive mode is for developing recipes. It allows to

- dynamically interleave execution with step definition by executing the latest added step
- overwrite steps (which throws an exception if not enabled explicitly)
- easily turn on/off interactive mode in order to not execute the interactive commands when running recipe in production
- execute a specific step and branch that the user might want to examine

In the following we will use the same example as in [Quickstart](#quickstart) but check the output at after each step.

#### Basic example


```python
import reflow as rf

# define recipe
recipe = rf.Recipe()

# enable interactive mode
dev_mode = True
if dev_mode:

    # allow overwriting/redefining of steps
    recipe.allow_overwrite = True

    # start an execution session with a fixed input and executor
    # Note: this is syntactic sugar and can be achieved by `recipe.__call__` alone
    dev = rf.Session(recipe).process("some input").using()

    # OPTIONAL:
    #   add `.using(cache_reset="step")` to recalculate the last step
    #   when executing the recipe; you can also use `cache_reset="all"`
    #   to recalculate everything
    # dev = rf.Session(recipe).process("some input").using(cache_reset="step")
```


```python
@recipe.option()
def step1___option1(x):
    return x + " > step1=option1"

# display result
if dev:
    result = dev.execute()
    display(result)
```


    'some input > step1=option1'



```python
@recipe.option()
def step1___option2(x):
    return x + " > step1=option2"
```


```python
@recipe.option()
def step2(x):
    return x + " > step2=default"

if dev:
    result = dev.execute()
    display(result)
```


    'some input > step1=option2 > step2=default'


#### Execute a specific steps and branches

You can execute a specific step and branch or a complete path whenever you need to, without having to redefine all the previous steps.


```python
# execute a specific step and branch history
if dev:
    result = dev.execute(step="step1", option="option1")
    display(result)
```


    'some input > step1=option1'



```python
# execute a specific step and branch history
if dev:
    result = dev.execute(include={"step1": "option1", "step2": "default"})
    display(result)
```


    'some input > step1=option1 > step2=default'


#### Caching

By default, the Session uses an executor that caches the results of all steps in memory (the `Cache` type can be customized and is easily extendible).
You can modify this behavior.


```python
import warnings
warnings.filterwarnings('ignore', category=UserWarning)
```


```python
# execute last step and branch to check the current execution path
if dev:
    result = dev.execute()
    display(result)
```


    'some input > step1=option2 > step2=default'



```python
# redefine `step2=default`
@recipe.option()
def step2(x):
    return x + " > step2=NEW"

# note that nothing changes because the result of the current step has been cached
# NOTE: this default behavior can be adjusted when starting the Session (see above)
if dev:
    result = dev.execute()
    display(result)

# you can also reset the cache for the latest step (or all steps) when executing
if dev:
    result = dev.execute(cache_reset="last step")
    display(result)
```


    'some input > step1=option2 > step2=default'



    'some input > step1=option2 > step2=NEW'



```python
# note that by default only the last step is cached
# so changes to other steps will show

# redefine `step1=option1`
@recipe.option()
def step1___option2(x):
    return x + " > step1=NEW"

# execute `step2` and notice that we still get the same result for `step1=option1`
if dev:
    result = dev.execute(step="step2")
    display(result)

# but when we reset the last step, the first step is also recalculated
if dev:
    result = dev.execute(step="step2", cache_reset="last step")
    display(result)
```


    'some input > step1=NEW > step2=NEW'



    'some input > step1=NEW > step2=NEW'


### Cached and Persistent Execution Results

Execution of paths are cached in memory by default. However, they can also be persisted. This can also help to resuse some of the results from earlier previous steps saving compute resources.

#### Steps are cached

Steps and options are cached in memory by default.


```python
import reflow as rf
import time

recipe = rf.Recipe()

@recipe.option()
def step1(x):
    time.sleep(5)
    return x + " > step1=default"

@recipe.option()
def step2___option1(x):
    time.sleep(5)
    return x + " > step2=option1"

@recipe.option()
def step2___option2(x):
    return x + " > step2=option2"
```


```python
recipe_input = "start"
run = rf.Session(recipe).process(recipe_input)
```


```python
%%time
# not cached, will take 10 seconds (step1: 5s, step2=option2: 5s)
run.execute(include={"step2": "option1"});
```

    CPU times: user 787 µs, sys: 2.53 ms, total: 3.31 ms
    Wall time: 5 s



```python
%%time
# cached, will return immediately since the complete path is cached
run.execute(include={"step2": "option1"});
```

    CPU times: user 208 µs, sys: 293 µs, total: 501 µs
    Wall time: 505 µs



```python
%%time
# note that only the complete path is cached by default
# thus executing `step1` will still take 5s
run.execute(step="step1");
```

    CPU times: user 2.64 ms, sys: 0 ns, total: 2.64 ms
    Wall time: 5 s



```python
%%time
# you can add `step1` to the cache when executing paths
run.execute(
    step="step1",
    cache_include=["step1"]  # short for [("step1", ".*")]
);
```

    CPU times: user 1.68 ms, sys: 549 µs, total: 2.23 ms
    Wall time: 5 s



```python
%%time
# then it will be reused in subsequent runs
# thus here step2=option2 return immediately
run.execute(
    include={"step2": "option2"},
    cache_include=["step1"]  # short for [("step1", ".*")]
);
```

    CPU times: user 1.1 ms, sys: 1.51 ms, total: 2.61 ms
    Wall time: 5 s


#### Persistence

Steps and options can be cached to disk or [MLflow](https://mlflow.org/).


```python
import reflow as rf
import tempfile

recipe = rf.Recipe()

@recipe.option()
def step1(x):
    return x + " > step1=default"

recipe_input = "start"

with tempfile.TemporaryDirectory() as out:
    cache = rf.FileCache(out)
    rf.Session(recipe).process(recipe_input).using(cache=cache)
    recipe(recipe_input, cache=cache)

with tempfile.TemporaryDirectory() as out:
    from reflow.cache.mlflow_cache import MlflowCache
    cache = MlflowCache(out)
    rf.Session(recipe).process(recipe_input).using(cache=cache)
```

### Parallel and distributed execution

TODO

### Flexible step definitions

`Recipe` provides several ways to define steps. This includes defining branches as any hashable data type, as well as some syntactic sugar.


```python
import reflow as rf
recipe = rf.Recipe()
recipe.allow_overwrite = True

# automatically names the step and branch by parsing the function name
# step and option are separated by `___`
# -> step1=option1
@recipe.option()
def step1___option1(x):
    return x + " > step1=option1"

# if no branch name is given it is set to "default"
# -> step1=default
@recipe.option()
def step1(x):
    return x + " > step1=default"

# manually set step and branch names
# -> step1=default
@recipe.option(step="step1", option="option1")
def some_function(x):
    return x + " > step1=option1"

# programatically set step and branch
# -> step1=default
recipe.add_option(lambda x: x + " > step1=default", "step1", "option1")

# you can also set a noop branch easily
# -> step1=NOOP
recipe.add_option_noop("step1")
```

You can also easily add batches of branches.

**Note** that branches do not have to be strings.


```python
import reflow as rf
recipe = rf.Recipe()

for i in range(3):
    @recipe.option(option=i)
    def step1(x, i=i):
        return x + f" > step1=option{i}"

recipe
```




    Recipe
        INPUT
            step1
                options:
                    0
                    1
                    2
                default_option: None
                latest_option:  2
        STEPS
        OUTPUT
            step1
                parents:
                    ROOT
                options:
                    0
                    1
                    2
                default_option: None
                latest_option:  2



### Easy sharing and combination of recipes

Recipes can be combined easily and defined as classes as well for easier sharing and reuse.


```python
# combining recipes
# TODO: add support for combining recipes

# recipe 1
recipe1 = rf.Recipe()

@recipe1.option()
def step1___option1(x):
    return x + " > step1=option1"

@recipe1.option()
def step1___option2(x):
    return x + " > step1=option2"

# recipe 2
recipe2 = rf.Recipe()

@recipe2.option()
def step2(x):
    return x + " > step2=default"

# inspect
print("Recipe 1:", recipe1)
print("Recipe 2:", recipe2)
print("Recipe 1 + Recipe 2:", recipe)
```

    Recipe 1: Recipe
        INPUT
            step1
                options:
                    option1
                    option2
                default_option: None
                latest_option:  option2
        STEPS
        OUTPUT
            step1
                parents:
                    ROOT
                options:
                    option1
                    option2
                default_option: None
                latest_option:  option2

    Recipe 2: Recipe
        INPUT
            step2
                options:
                    default
                default_option: None
                latest_option:  default
        STEPS
        OUTPUT
            step2
                parents:
                    ROOT
                options:
                    default
                default_option: None
                latest_option:  default

    Recipe 1 + Recipe 2: Recipe
        INPUT
            step1
                options:
                    0
                    1
                    2
                default_option: None
                latest_option:  2
        STEPS
        OUTPUT
            step1
                parents:
                    ROOT
                options:
                    0
                    1
                    2
                default_option: None
                latest_option:  2



### Extending recipe


```python
# you can define recipes as a class without polluting the global space with functions
class MyRecipe(rf.Recipe):

    def init_steps(self):

        @self.option()
        def step1___option1(x):
            return x + " > step1=option1"

        @self.option()
        def step1___option2(x):
            return x + " > step1=option2"

recipe = MyRecipe()
recipe("test")
```




    'test > step1=option2'




```python
# you can also parameterize your recipes like this
# but I am not sure that this is a good idea, yet :D

class MyRecipe(rf.Recipe):

    def __init__(self, param:str="None"):
        super().__init__()
        self.param = "HALLO"

    def something_fancy(self, x):
        return f"something fancy({self.param}, x)"

    def init_steps(self):

        @self.option()
        def step1___option1(x):
            return x + " > step1=option1"

        @self.option()
        def step1___option2(x):
            return self.something_fancy(x) + " > step1=option2"

recipe = MyRecipe()
recipe("test")
```




    'something fancy(HALLO, x) > step1=option2'



## Potential alternatives


| Framework                   | Params / Branches | Caching | Parallel | DAG | Open Source | Last Update                                                                                   |
| --------------------------- | ----------------- | ------- | -------- | --- | ----------- | --------------------------------------------------------------------------------------------- |
| **PrupleML (Recipe)** [^rp] | ❔                | ❔      | ❔       | ❌  | ❔          | ![repo](https://img.shields.io/github/last-commit/mgbckr/purpleml)                            |
| MLflow (recipies) [^mlflow] | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/mlflow/mlflow)                              |
| Link [^link]                | ❔                | ✅      | ✅       | ❔  | ❌          | [![pypi](https://img.shields.io/pypi/v/mrx_link)](https://pypi.org/project/mrx-link/#history) |
| pydags [^pyd]               | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/DavidTorpey/pydags)                         |
| paradag [^par]              | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/xianghuzhao/paradag)                        |
| fn graph [^fn1][^fn2][^fn3] | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/BusinessOptics/fn_graph)                    |
| graphtik [^gt]              | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/pygraphkit/graphtik)                        |
| schedula [^sc]              | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/vinci1it2000/schedula)                      |
| Airflow [^af]               | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/apache/airflow)                             |
| Luigi [^lui]                | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/spotify/luigi)                              |
| dagster [^dg]               | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/dagster-io/dagster)                         |
| Kubeflow [^kfl]             | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/kubeflow/kubeflow)                          |
| hamilton [^ham]             | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/DAGWorks-Inc/hamilton)                      |
| [tawazi](#tawazi) [^taw]    | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/mindee/tawazi)                              |
| [ploomber](#ploomber) [^pl] | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/ploomber/ploomber)                          |
| snakemake [^sm]             | ❔                | ❔      | ❔       | ❔  | ❔          | ![repo](https://img.shields.io/github/last-commit/snakemake/snakemake)                        |

**Note:** Question marks (❔) need to be filled in.

[^rp]: https://github.com/mgbckr/purpleml
[^mlflow]: https://mlflow.org/docs/latest/recipes.html
[^link]: https://link.makinarocks.ai/
[^pyd]: https://github.com/DavidTorpey/pydags
[^fn1]: https://fn-graph.businessoptics.biz/
[^fn2]: https://towardsdatascience.com/fn-graph-lightweight-pipelines-in-python-121f8d5f9ef6
[^fn3]: https://github.com/BusinessOptics/fn_graph
[^taw]: https://mindee.github.io/tawazi/
[^gt]: https://github.com/pygraphkit/graphtik
[^sc]: https://schedula.readthedocs.io/en/master/
[^pl]: https://ploomber.io/
[^ham]: https://github.com/DAGWorks-Inc/hamilton
[^par]: https://pypi.org/project/paradag
[^sm]: https://snakemake.readthedocs.io/en/stable/
[^dg]: https://dagster.io/
[^af]: https://airflow.apache.org/
[^lui]: https://luigi.readthedocs.io/en/stable/
[^kfl]: https://www.kubeflow.org/

### Tawazi

- this is pretty nice! but no branches?
- parameters possible though (so we could do a big if/else for example ...they also have branches ... somewhat)
- alive!

### Ploomber

- can do parameterization: https://docs.ploomber.io/en/latest/user-guide/parametrized.html
- can do grid search: https://docs.ploomber.io/en/latest/cookbook/grid.html
  - not sure about caching
- YAML based option/parameter selection

## Roadmap

### Potential directions / thoughts

#### General

- [ ] allow to easily save / share recipe execution results
- [ ] advanced visualization capabilities
- [ ] interactive visualization (recalculate, empty cache, etc.)
- [ ] integrate with one of the DAG execution frameworks like AirFlow?

#### Parameter support

- [ ] allow kwargs for functions? maybe as parameters of the whole Recipe?
    - [ ] How to handle those in cache?

- [ ] explicitly support parameters
    - [ ] combine into special kind of node (batch/grid)?
    - [ ] is it necessary given the current functionality
            to define arbitrary values for branch names?

- [x] batches/grids
    - [/] implement batches (see docs)
    - [/] implement grids?
    - [x] easily achievable with a for-loop already

### Planned

#### Advanced caching / functionality

- [ ] **Emit statistics:** allow steps to emmit stats (not part of the regular return value but optional)
    which can be used by a cache to show nice results (e.g., MLflow)
- [ ] **Split caching/loading**: Allow to split / load different types of the cache
    (actually is already possible by splitting a step and then caching each split)
- [ ] Add asserts (do not continue / default result when not possible)

#### DAG Support

In practice it turns out that this is **SUPER IMPORTANT**. This has to happen soon.

- [x] allow DAGs and multi-inputs (should actually work)
    - [x] move to DAG rather than dictionary as underlying step data structure
    - [x] allow multiple inputs (could be complicated due to parameter name parsing)
    - [x] parameters give names in decorators
            (but not necessarily, could be ordered by default)
    - [ ] only visualize step DAGs not branches?

- [ ] nested recipes
- [ ] for parallel execution: merge nodes with only one child?

#### Syntactic sugar


- [ ] add method to produce code that executes the recipe as sequential function calls
- [ ] maybe enforce naming scheme so make sure
    we can convert at the end (make optional)?

### In progress

#### MVP

- [x] Milestone 1: Basic functionality

    - [x] move from `default` to `enabled`?
    - [x] path-wise execution
    - [x] parallel execution
    - [x] merge recipes (makes them shareable!)
    - [x] allow "add after" (I decided not to implement this
            as it might interfere with other "before" or "after" directives,
            we could explicitly set parents, but that seems redundant
            ... just run the whole recipe again)
    - [x] revamp config for advanced execution filter (which paths are executed)

- [x] Milestone 2: Recipe execution
    - [x] breadth first execution
        - [x] set layers that make sense to parallelize explicitly
    - [x] allow "insert before" operation
    - [x] add execution filtering
    - [x] add cache filtering
    - [x] clean-up execution functionality (as we use the library)
        - [x] latest
        - [x] interactive?
    - [x] add randomization
        - [x] depth first
        - [x] breadth first
        - [x] cache level (latest? ... implement as we use it)
    - [x] add parallelization filtering for breadth first execution
    - [x] somehow default to caching only last step (may be a bit janky)
    - [x] cache (needs locking if we execute path-wise ... not good)
        - [x] dict cache backend
        - [x] test
        - [x] allow disabling cache for steps and branches
        - [x] default cached or not as Recipe option?
        - [x] partial execution (prune paths with cached child)
            - [x] depth first
            - [x] breadth first
        - [x] file cache backend
            - [x] make sure it is easy to unlikely that things overwrite
        - [x] mlflow cache backend (for distributed programming)

- [x] Milestone 3: Clean-up and syntactic sugar
    - [x] test while doing project
    - [x] move cache filtering to executor
    - [x] make interactive an executor with more flexible cache
        - executors should already provide all functionality and
            the current interactive implementation seems redundant
        - will allow interactive to use a custom cache
    - [x] check/verify and then revamp Interactive framework
        - [x] create wrapper ("Kitchen", "Run", **"Session"**) of recipe, input, and executor
        - [x] make Interactive into a special kind of executor (with no execution)
        - [x] add syntactic sugar to Recipe:
                (recipe(*input, executor=None).execute(execute_filter, cache_filter))
    - [x] check / allow non-string branch names for rudimentary param support

- [x] Milestone 4: Syntactic sugar
    - [x] defined functions are branch-able
        (`branch` keyword on function call switched branch)
    - [x] recipe is now callable like a function
        and can select steps and branches dynamically
    - [x] Nice session handling:
        `Session(recipe).process(input).using(executor).execute()`
    - [x] testing

- [ ] Milestone 5: Distributed/parallel execution
    - [x] individual cache for different steps
    - [x] test parallel execution
    - [ ] prevent redundant distributed execution
        - [ ] enable locking in caches
        - [ ] implement locking in DepthFirstExecutor (I don't think it makes sense in BreadthFirstExecutor?)
        - [ ] filter before going into joblib Parallel?
    - [ ] test distributed execution
        - [ ] Simple MLflow
        - [ ] Spark
        - [ ] Dask

- [ ] Milestone 6: Documentation and clean-up
    - [x] add alternatives
    - [ ] overview / quick start
    - [ ] write about features
    - [ ] code documentation
    - [ ] re-document features
    - [x] rename `branch` to `option` ?
    - [ ] clean up / refactor `recipe.py`
    - [ ] refactor all `utils.py`
    - [ ] allow for easy removal (code auto generation)

- [ ] Initial Release


## Abandoned ideas

### Context manager syntax

I was considering a context manager syntax. Like so:

```python
r = Recipe()

with r.option("step1.1", parents=Recipe.DAG_ROOT) as step:

    @step.option():
    def step1_1___option1(x):
        return x + "_step1.1=option1"

with r.option("step1.2", parents=Recipe.DAG_ROOT) as step:

    @step.option():
    def step1_2___option1(x):
        return x + "_step1.2=option1"

    @step.option():
    def step1_2___option2(x):
        return x + "_step1.2=option1"

with r.option("step2", parents=["step1.1", "step1.2"]) as step:

    @step.option():
    def step2___option1(x1, x2):
        return x + "_step2=option1"
```

Can be implemented as mix-in to Recipe (like the `Callable` mix-in).


However:

- This would have been for better readability only since everything can already be achieved by the current methods.
- And more importantly, in a Jupyter Notebook setting I may want to define each option in a separate cell which means I can not use the context manager syntax. In that case, we would have two syntax variants which you have to learn depending in which environment you are in. This is a cognitive overhead that I would like to avoid.

Overall, this makes the context manager syntax nice to look at but ultimately not useful enough to be implemented and possibly confusing.
Thus, I will not further pursue this feature.
