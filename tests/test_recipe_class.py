def test_recipe_class():
    import reflow as rp

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
    

if __name__ == '__main__':
    test_recipe_class()
