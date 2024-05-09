from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project
from kedro.framework.startup import bootstrap_project
from airflow.decorators import dag, task
from kedro.io import MemoryDataset
from kedro.runner import SequentialRunner
from kedro.framework.project import pipelines

def run_kedro_node(package_name, pipeline_name, node_name, project_path, env, *args):
    configure_project(package_name)
    with KedroSession.create(project_path, env) as session:
            datasets = {}
            for x in args:
                datasets.update(x)
                
            if isinstance(node_name, str):
                node_name = [node_name]

            context = session.load_context()
            catalog = context.catalog
            for j, ds in datasets.items():
                catalog.add(j, MemoryDataset(ds))
            
            runner = SequentialRunner()
            pipeline = pipelines[pipeline_name].filter(node_names=node_name)
            output = runner.run(pipeline, catalog )
            return output
# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "space"

# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(
    dag_id="space3",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval="@once",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
    ),
)
def kedro_dag():
    @task(task_id="preprocess-companies-node")
    def preprocess_companies_node(*args):
        return run_kedro_node(package_name, pipeline_name, "preprocess_companies_node", project_path, env, *args)
    
    @task(task_id="preprocess-shuttles-node")
    def preprocess_shuttles_node(*args):
        return run_kedro_node(package_name, pipeline_name, "preprocess_shuttles_node", project_path, env, *args)

    @task(task_id="create-model-input-table-node")
    def create_model_input_table_node(*args):
        return run_kedro_node(package_name, pipeline_name, "create_model_input_table_node", project_path, env, *args)

    @task(task_id="split-data-node")
    def split_data_node(*args):
        return run_kedro_node(package_name, pipeline_name, "split_data_node", project_path, env, *args)
    
    @task(task_id="train-model-node")
    def train_model_node(*args):
        return run_kedro_node(package_name, pipeline_name, "train_model_node", project_path, env, *args)

    @task(task_id="evaluate-model-node")
    def evaluate_model_node(*args):
        return run_kedro_node(package_name, pipeline_name, "evaluate_model_node", project_path, env, *args)

    ds1 = preprocess_companies_node()
    ds2 = preprocess_shuttles_node()
    mit = create_model_input_table_node(ds1, ds2) 
    x = split_data_node(mit) 
    y = train_model_node(x)
    evaluate_model_node(x, y)
    
kedro_dag()