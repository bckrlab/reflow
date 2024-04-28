import tempfile


def test_file_cache():
    from reflow.cache.file_cache import FileCache

    with tempfile.TemporaryDirectory() as tmpdirname:
        cache = FileCache(out_path=tmpdirname)

        cache.set("test", {"test": "option1"}, "test")
        assert cache.get("test", {"test": "option1"}) == "test"
        assert len(list(cache.items())) == 1

        cache.set("test", {"test": "option1"}, "test")
        assert cache.get("test", {"test": "option1"}) == "test"
        assert len(list(cache.items())) == 1

        cache.set("test", {"test": "option1"}, "test2")
        assert cache.get("test", {"test": "option1"}) == "test2"
        assert len(list(cache.items())) == 1

        cache.set("test", {"test": "option2"}, "test3")
        assert cache.get("test", {"test": "option2"}) == "test3"
        assert len(list(cache.items())) == 2


def test_file_cache_recipe_parallel():
    from reflow import Recipe, Session
    from reflow.cache.file_cache import FileCache

    with tempfile.TemporaryDirectory() as tmpdirname:
        cache = FileCache(out_path=tmpdirname)

        r = Recipe()

        @r.option()
        def test___default(x):
            return x + " (processed)"

        Session(r).process("blubb").using(cache=cache, n_jobs=2).execute(squeeze=False)


if __name__ == "__main__":
    test_file_cache_recipe_parallel()
