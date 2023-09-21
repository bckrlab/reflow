def test_recipe_params():
    import reflow as rp

    recipe = rp.Recipe()

    @recipe.option(option=1.0)
    def step1(x):
        return x + " > step=1.0"

    @recipe.option(option=1.5)
    # flake8: noqa: F811
    def step1(x):
        return x + " > step=1.5"

    @recipe.option()
    def step2(x):
        return x + " > step2=default"

    options, test = recipe("test", squeeze=False)[0]
    assert "step1" in options and options["step1"] == 1.5

    assert test["step2"] == "test > step=1.5 > step2=default"


if __name__ == "__main__":
    test_recipe_params()
