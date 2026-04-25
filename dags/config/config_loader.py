import yaml
import os

BASE_PATH = os.path.dirname(__file__)

def load_pipelines():
    file_path = os.path.join(BASE_PATH, "pipelines.yml")

    with open(file_path, "r") as f:
        return yaml.safe_load(f)