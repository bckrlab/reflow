import logging

import reflow as rp

logging.basicConfig(level=logging.DEBUG)


def test_recipe_basic():
    import reflow as rp

    # define recipe

    recipe = rp.Recipe()

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

    # execute recipe and look at results
    recipe_input = "start"
    r1 = "start_step1=option1_step2=default"
    r2 = "start_step1=option2_step2=default"

    result_latest = recipe(recipe_input)
    assert result_latest == r2

    result_all = recipe(recipe_input, include=None)
    assert len(result_all) == 2
    r = [r["step2"] for _, r in result_all]
    assert r1 in r and r2 in r

    try:
        recipe(recipe_input, include=[{"step1": "opt"}])
        assert False
    except ValueError:
        assert True

    result = recipe(recipe_input, include=[{"step1": "opt"}], on_purge_step="ignore")
    assert result == "start_step2=default"

    result = recipe(recipe_input, include=[{"step1": "option1"}])
    assert result == r1

    result = recipe(recipe_input, step="step1", include=[{"step1": "option1"}])
    assert result == "start_step1=option1"

    result = recipe(recipe_input, step="step2", include="step")
    assert result == "start_step2=default"

    assert step1___option1(recipe_input) == "start_step1=option1"
    assert step1___option1(recipe_input, option="option2") == "start_step1=option2"

    assert step1___option2(recipe_input) == "start_step1=option2"
    assert step1___option2(recipe_input, option="option1") == "start_step1=option1"

    df = rp.results_to_dataframe(result_all, filter_columns=False)
    print(df)


def test_recipe_insert():
    from reflow.recipe import Recipe

    def default_recipe():
        recipe = Recipe()

        recipe.insert_step("step1")
        recipe.insert_step("step2")
        recipe.insert_step("step3")

        return recipe

    pass

    recipe = default_recipe()
    assert list(recipe.steps.nodes)[1:] == ["step1", "step2", "step3"]


def test_recipe_fancy_functions():
    # TEST
    recipe = rp.Recipe()

    @recipe.option()
    def step1(input_str):
        # sleep(5)
        return f"{input_str}_step1=default"

    @recipe.option()
    def step1___default2(input_str):
        return f"{input_str}_step1=default2"

    assert step1("test") == "test_step1=default"
    assert step1("test", option="default") == "test_step1=default"
    assert step1("test", option="default2") == "test_step1=default2"
    assert step1___default2("test") == "test_step1=default2"
    assert step1___default2("test", option="default") == "test_step1=default"
    assert step1___default2("test", option="default2") == "test_step1=default2"


# def _test_recipe_combine(recipe_cls):

#     r1 = recipe_cls()
#     r1.add_branch(lambda x: x + "step1.1", "step1.1", "option1")

#     r2 = recipe_cls()
#     r3 = r1 + r2

#     assert len(r3.options) == 1

#     r2.add_branch(lambda x: x + "step2.1", "step2.1", "option1")
#     r3 = r1 + r2

#     assert len(r3.options) == 2

# def test_recipe_combine():

#     from reflow.recipe import Recipe
#     _test_recipe_combine(Recipe)
#     _test_recipe_combine(rp.Recipe)


if __name__ == "__main__":
    test_recipe_fancy_functions()
    pass
