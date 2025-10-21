%pip install databricks-sdk==0.68.0
%restart_python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings as Job
from databricks.sdk.service.jobs import JobCluster
import os
from util.cluster_spec import *

cluster_spec = d4sv3_1w_tot_8c_32g
cluster_name = cluster_spec.as_dict()['custom_tags']['resource_type']                        
job_cluster_key= cluster_spec.as_dict()['custom_tags']['job_cluster_key'] 

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
            {"job_cluster_key": job_cluster_key, 
             "new_cluster":cluster_spec.as_dict()
            }).as_dict() 
        ],
        "tags": {
            "resource_type": cluster_name,
            "cluster_name": cluster_name
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
            {"job_cluster_key": job_cluster_key, 
             "new_cluster":cluster_spec.as_dict()
            }).as_dict() 
        ],
        "tags": {
            "resource_type": cluster_name,
            "cluster_name": cluster_name
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

print(f"DDL setup job id: {ddl_setup_id}")
print(f"Daily load job id: {daily_load_id}")



#end of page