%pip install databricks-sdk==0.68.0
%restart_python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job
from databricks.sdk.service.jobs import JobCluster
import os
from util.cluster_spec import d4sv3_single_tot_4c_16g

cluster_spec = d4sv3_single_tot_4c_16g

ddl_setup = Job.from_dict(
    {
        "name": "ddl_setup",
        "tasks": [
            {
                "task_key": "create_tables",
                "notebook_task": {
                    "notebook_path": os.path.abspath("./setup/ddl"),
                    "source": "WORKSPACE"
                }
            }
        ],
        "queue": {
            "enabled": True,
        },
        "job_clusters" : [JobCluster.from_dict(
            {"job_cluster_key": "d4sv3", 
             "new_cluster":cluster_spec.as_dict()
            }).as_dict() 
        ],
        "tags": {
            "resource_type": "d4sv3_single",
            "cluster_name": "d4sv3_single_4c_16g"
        },
        "parameters": [
            {"name": "CATALOG", "default": "1"},
            {"name": "SCHEMA", "default": "1"},
            {"name": "PROFILE", "default": "1"},
            {"name": "PROFILE_DTL", "default": "1"}
        ]
    }
)

daily_load = Job.from_dict(
    {
        "name": "daily_load",
        "tasks": [
            {
                "task_key": "append_nyc_taxi",
                "notebook_task": {
                    "notebook_path": os.path.abspath("./transform/append_nyc_taxi"),
                    "source": "WORKSPACE"
                }
            },
            {
                "task_key": "merge_summary_data",
                "depends_on": [
                    {
                        "task_key": "append_nyc_taxi"
                    }
                ],
                "notebook_task": {
                    "notebook_path": os.path.abspath("./transform/merge_summary"),
                    "source": "WORKSPACE"
                }
            }
        ],
        "queue": {
            "enabled": True,
        },
        "job_clusters" : [JobCluster.from_dict(
            {"job_cluster_key": "d4sv3", 
             "new_cluster":cluster_spec.as_dict()
            }).as_dict() 
        ],
        "tags": {
            "resource_type": "d4sv3_single",
            "cluster_name": "d4sv3_single_4c_16g"
        },
        "parameters": [
            {"name": "CATALOG", "default": "1"},
            {"name": "SCHEMA", "default": "1"},
            {"name": "YEAR_START", "default": "1"},
            {"name": "YEAR_END", "default": "1"},
            {"name": "MONTH_START", "default": "1"},
            {"name": "MONTH_END", "default": "1"},
            {"name": "PROFILE", "default": "1"},
            {"name": "PROFILE_DTL", "default": "1"}
        ]
    }
)
w = WorkspaceClient()
ddl_setup_id = w.jobs.create(**ddl_setup.as_shallow_dict())
daily_load_id = w.jobs.create(**daily_load.as_shallow_dict())
print(f"Daily load job id: {daily_load_id}")