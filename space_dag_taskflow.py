from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


def run_kedro_node(package_name, pipeline_name, node_name, project_path, env):
    configure_project(package_name)
    with KedroSession.create(project_path, env=env) as session:
        return session.run(pipeline_name, node_names=[node_name])
    
# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "space"

@dag(
    dag_id="space_tf",
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
        retries=1,
        retry_delay=timedelta(minutes=5)    
    )
)
def kedro_dag():
    @task(task_id="preprocess-companies-node")
    def preprocess_companies_node():
        run_kedro_node(package_name, pipeline_name, "preprocess_companies_node", project_path, env)
    
    @task(task_id="preprocess-shuttles-node")
    def preprocess_shuttles_node():
        run_kedro_node(package_name, pipeline_name, "preprocess_shuttles_node", project_path, env)

    @task(task_id="create-model-input-table-node")
    def create_model_input_table_node(ds = None):
        run_kedro_node(package_name, pipeline_name, "create_model_input_table_node", project_path, env)

    @task(task_id="split-data-node")
    def split_data_node():
        run_kedro_node(package_name, pipeline_name, "split_data_node", project_path, env)
    
    @task(task_id="train-model-node")
    def train_model_node():
        run_kedro_node(package_name, pipeline_name, "train_model_node", project_path, env)

    @task(task_id="evaluate-model-node")
    def evaluate_model_node():
        run_kedro_node(package_name, pipeline_name, "evaluate_model_node", project_path, env)

    preprocess_shuttles_node()  >> create_model_input_table_node() 
    preprocess_companies_node() >> create_model_input_table_node()
    create_model_input_table_node() >> split_data_node() >> train_model_node() >> evaluate_model_node()   
    

kedro_dag()