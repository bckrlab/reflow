def test_cache():
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
    # r1 = "start_step1=option1_step2=default"
    r2 = "start_step1=option2_step2=default"

    dev = rp.Session(recipe, default_cache_reset=None).process(recipe_input)
    recipe.allow_overwrite = True

    result_latest = dev.execute()
    assert result_latest == r2

    # redefine `step2=default`
    @recipe.option()
    # flake8: noqa: F811
    def step2(x):
        return x + "_step2=NEW"

    # note that nothing changes because the result of the current step has been cached
    # NOTE: this default behavior can be adjusted when starting the Session (see above)
    result = dev.execute()
    assert result == r2

    # you can also reset the cache for the latest step (or all steps) when executing
    result = dev.execute(cache_reset="last step")
    assert result == "start_step1=option2_step2=NEW"

    # note that by default only the last step is cached
    # so changes to other steps will show
    # redefine `step1=option2`
    @recipe.option()
    def step1___option2(x):  # flake8: noqa: F811
        return x + "_step1=NEW"

    print(dev.executor.cache.keys())

    # execute `step2` and notice that we still get the same result for `step1=option1`
    result = dev.execute(step="step2", cache_include="step2")
    assert result == "start_step1=option2_step2=NEW"

    # but when we reset the last step, the first step is also recalculated
    result = dev.execute(step="step2", cache_include="step2", cache_reset="last step")
    assert result == "start_step1=NEW_step2=NEW"


if __name__ == "__main__":
    test_cache()
