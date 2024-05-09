from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import logging

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.decorators import dag, task

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project, pipelines
from kedro.framework.startup import bootstrap_project
from kedro.io import MemoryDataset
from kedro.runner import SequentialRunner


class KedroOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str | list[str],
        project_path: str | Path,
        env: str,
        prev: list[str] | None,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env
        self.prev = prev or []

    def execute(self, context):
        # configure_project(self.package_name)
        bootstrap_project(self.project_path)
        ti = context["task_instance"]
        
        with KedroSession.create(self.project_path, env=self.env) as session:
            # logging.warn(self.task_id).xcom_pull()
            if isinstance(self.node_name, str):
                self.node_name = [self.node_name]

            context = session.load_context()
            catalog = context.catalog
            for i in self.prev:
                x = ti.xcom_pull(task_ids=i)
                for j, ds in x.items():
                    catalog.add(j, MemoryDataset(ds))
            
            runner = SequentialRunner()
            pipeline = pipelines[self.pipeline_name].filter(node_names=self.node_name)
            output = runner.run(pipeline, catalog )
            return output


# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "space"

# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(dag_id="trying")
def space_dag():
    x = KedroOperator(
        task_id="preprocess_companies",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="preprocess_companies_node",
        project_path=project_path,
        env=env,
        prev=None,
    )
    y = KedroOperator(
        task_id="preprocess_shuttles",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="preprocess_shuttles_node",
        project_path=project_path,
        env=env,
        prev=None,
    )
    z = KedroOperator(
        task_id="create_model_input_table",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="create_model_input_table_node",
        project_path=project_path,
        env=env,
        prev=["preprocess_companies", "preprocess_shuttles"],
    )

    x>>y>>z




space_dag()


# Pass along memory datasets through xcom