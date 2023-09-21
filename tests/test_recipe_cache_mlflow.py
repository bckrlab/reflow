import tempfile


def test_cache_mlflow_file_based():
    from mlflow.tracking import MlflowClient

    from reflow.cache.mlflow_cache import MlflowCache

    with tempfile.TemporaryDirectory() as tmpdirname:
        client = MlflowClient(tmpdirname)
        cache = MlflowCache("test", mlflow_client=client)

        path = "step1", {"step1": "branch1", "step2": "branch3", "step3": "branch1.1"}

        value = "BLUBB"
        cache.set(*path, value)
        assert cache.get(*path) == value

        value = "BLUBB2"
        cache.set(*path, value)
        assert cache.get(*path) == value

        assert cache.contains(*path)

        cache.clear()
        assert not cache.contains(*path)

        value = "BLUBB2"
        cache.set(*path, value)
        assert cache.contains(*path)
        cache.delete(*path)
        assert not cache.contains(*path)

        value = "BLUBB2"
        cache.set(*path, value)
        assert cache.contains(*path)
        cache.delete_all("step1")
        assert not cache.contains(*path)

        assert len(list(cache.items())) == 0

        path2 = "step2", dict(
            [("step1", "branch1"), ("step2", "branch3"), ("step3", "branch1.2")]
        )
        value = "BLUBB2"
        cache.set(*path, value)
        cache.set(*path2, value)
        assert cache.contains(*path)

        assert len(list(cache.items())) == 2

        cache.delete_all("step1", "branch1")
        assert not cache.contains(*path)
        assert cache.contains(*path2)

        pass


# def test_cache_mlflow_client_remote():  # not sure how to test this
#     from pathlib import Path
#     from mlflow import MlflowClient

#     # Create an experiment with a name that is unique and case sensitive.
#     client = MlflowClient("HTTP://localhost:5000")
#     try:
#         experiment_id = client.create_experiment("purpleml_recipe_test")
#     except:
#         experiment_id = client.get_experiment_by_name("purpleml_recipe_test")\
#           .experiment_id

#     # Fetch experiment metadata information
#     experiment = client.get_experiment(experiment_id)
#     print("Name: {}".format(experiment.name))
#     print("Experiment_id: {}".format(experiment.experiment_id))
#     print("Artifact Location: {}".format(experiment.artifact_location))
#     print("Tags: {}".format(experiment.tags))
#     print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))

#     r = client.create_run(experiment_id=experiment_id)
#     client.log_artifact(r.info.run_id, "./README.md")

# def test_cache_mlflow_remote():  # not sure how to test this

#     from reflow.cache.mlflow_cache import MlflowCache
#     print("init")
#     cache = MlflowCache("purpleml_recipe_test", mlflow_client="http://localhost:5000")

#     # set cache
#     print("set")
#     cache.set([("test", "test")], "test")
#     cache.set([("test2", "test2")], "test2")

#     # get cache
#     print("get")
#     cache.get([("test", "test")])

#     print("list keys")
#     print(list(cache.keys()))

#     print("list items")
#     print(list(cache.items()))


if __name__ == "__main__":
    # test_cache_mlflow()
    test_cache_mlflow_file_based()
    pass
