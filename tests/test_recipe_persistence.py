

def test_recipe_persistence():
    
    import tempfile
    import pathlib
    import reflow as rp
    import reflow.persistence as rpp

    class MyRecipe(rp.Recipe):
        
        def init_steps(self):

            @self.option()
            def test1():
                return "Blubb 1"

            @self.option()
            def test2 (x):
                return x + " > Blubb 2"

    recipe = MyRecipe()
    dev = rp.Session(recipe).process()
    r = dev.execute()

    assert r  == "Blubb 1 > Blubb 2"

    with tempfile.TemporaryDirectory() as tmpdirname:

        path = pathlib.Path(tmpdirname)

        f = path / "test.pickle"
        rpp.save(recipe, f)

        recipe2 = rpp.load(f)

    dev2 = rp.Session(recipe2).process()
    r2 = dev2.execute()
    
    assert r == r2
    assert str(recipe) == str(recipe2)


if __name__ == '__main__':
    test_recipe_persistence()
