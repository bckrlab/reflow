def test_recipe_add_step():
    from reflow.recipe import Recipe

    r = Recipe()

    r.add_step("step1")

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step1"

    r.add_step("step2")

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step2"

    # split workflow
    r.add_step("step2.1", parents=["step1"])
    assert len(r.output_steps()) == 2
    assert r.output_steps()[0] == "step2"
    assert r.output_steps()[1] == "step2.1"

    r.add_step("step3", parents=["step2", "step2.1"])

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step3"

    print(r)


def test_recipe_dag_add_option():
    from reflow.recipe import Recipe

    r = Recipe()

    r.add_step("step1")

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step1"

    r.add_option(lambda x: x, step="step2")

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step2"

    # split workflow
    r.add_step("step2.1", parents=["step1"])
    r.add_option(lambda x: x, step="step2.1")
    assert len(r.output_steps()) == 2
    assert r.output_steps()[0] == "step2"
    assert r.output_steps()[1] == "step2.1"

    r.add_step("step3", parents=["step2", "step2.1"])

    assert len(r.input_steps()) == 1 and r.input_steps()[0] == "step1"
    assert len(r.output_steps()) == 1 and r.output_steps()[0] == "step3"

    print(r)
    pass


def test_recipe_dag_decorator():
    from reflow.recipe import Recipe

    r = Recipe()

    @r.option()
    def step1(x):
        return x + "_step1=default"

    @r.option()
    def step2___option1(x):
        return x + "_step2=option1"

    r.add_step("step1.1", parents=Recipe._DAG_ROOT)

    @r.option(step="step1.1")
    def step1_1(x):
        return x + "_step1.1=default"

    r.add_step("step3", parents=["step1", "step1.1"])

    @r.option()
    def step3___option1(x):
        return x + "_step3=option1"

    assert len(r.input_steps()) == 2
    assert r.input_steps()[0] == "step1" and r.input_steps()[1] == "step1.1"

    assert len(r.output_steps()) == 2
    assert r.output_steps()[0] == "step2" and r.output_steps()[1] == "step3"

    print(r)
    pass


def test_recipe_dag_execution_path():
    from reflow.recipe import Recipe

    r = Recipe()

    r.add_step(step="step-1", parents=None)
    r.add_step(step="step0.1", parents=None)
    r.add_step(step="step0.2", parents=None)
    r.add_step(step="step1", parents=["step0.1", "step0.2"])
    r.add_option(lambda x: x, step="step2")

    r.add_step("step3", parents=["step1"])
    r.add_step("step4")
    r.add_step("step5")

    r.add_step("step6", parents=["step2", "step5"])

    from reflow.utils.execution import execution_path_eager, execution_path_lazy

    p = execution_path_lazy(r)
    print(p)
    assert p == [
        [["step0.1"], ["step0.2"]],
        [["step1"]],
        [["step2"], ["step3", "step4", "step5"]],
        [["step-1"], ["step6"]],
    ]

    p = execution_path_eager(r)
    print(p)
    assert p == [
        [["step-1"], ["step0.1"], ["step0.2"]],
        [["step1"]],
        [["step2"], ["step3", "step4", "step5"]],
        [["step6"]],
    ]

    pass


def test_recipe_dag_execution_filter():
    from reflow.recipe import Recipe

    r = Recipe()

    r.add_step(step="step-1", parents=None)
    r.add_option(lambda x: x)

    r.add_step(step="step0.1", parents=None)
    r.add_option(lambda x: x)
    r.add_step(step="step0.2", parents=None)
    r.add_option(lambda x: x)
    r.add_step(step="step1", parents=["step0.1", "step0.2"])
    r.add_option(lambda x: x)

    r.add_option(lambda x: x, step="step2")

    r.add_step("step3", parents=["step1"])
    r.add_option(lambda x: x)
    r.add_step("step4")
    r.add_option(lambda x: x)
    r.add_step("step5")
    r.add_option(lambda x: x)

    r.add_step("step6", parents=["step2", "step5"])
    r.add_option(lambda x: x)

    from reflow.utils.utils import filter_recipe

    rr = filter_recipe(
        r, include={"step-1": ".*"}, exclude={"step-1": ".*"}, on_purge_step="warn"
    )

    from reflow.utils.execution import execution_path_eager

    p = execution_path_eager(rr)

    print(p)
    assert p == [
        [["step0.1"], ["step0.2"]],
        [["step1"]],
        [["step2"], ["step3", "step4", "step5"]],
        [["step6"]],
    ]

    pass


def test_recipe_dag_call():
    from reflow.callable import CallableRecipe as Recipe

    r = Recipe()

    @r.option()
    def step1(x):
        return x + "_step1=default"

    @r.option()
    def step2___option1(x):
        return x + "_step2=option1"

    @r.option()
    def step2___option2(x):
        return x + "_step2=option2"

    @r.option()
    def step3___option1(x):
        return x + "_step3=option1"

    print(r)

    result = r("test")
    print(result)

    pass


def test_recipe_dag_session():
    from reflow.callable import CallableRecipe as Recipe
    from reflow.session import Session

    r = Recipe()

    dev = Session(r).process("test")

    @r.option()
    def step1(x):
        return x + "_step1=default"

    res = dev.execute()
    print(res)

    @r.option()
    def step2___option1(x):
        return x + "_step2=option1"

    res = dev.execute()
    print(res)

    @r.option()
    def step2___option2(x):
        return x + "_step2=option2"

    res = dev.execute()
    print(res)

    @r.option()
    def step3___option1(x):
        return x + "_step3=option1"

    res = dev.execute()
    print(res)

    print(r)

    result = r("test")
    print(result)

    res = dev.execute(include="all")
    print(res)

    pass


if __name__ == "__main__":
    test_recipe_add_step()
    test_recipe_dag_add_option()
    test_recipe_dag_decorator()
    test_recipe_dag_execution_path()
    test_recipe_dag_execution_filter()
    test_recipe_dag_call()
    test_recipe_dag_session()
