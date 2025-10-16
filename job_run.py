import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# DDL setup job id: CreateResponse(job_id=861563541071827)
# Daily load job id: CreateResponse(job_id=737591956135053)

# ddl_setup_response = w.jobs.run_now(job_id=357646783472642 , 
#                     job_parameters = {"CATALOG": "workspace",
#                                         "SCHEMA": "default"}              
#                     )

daily_load_response = w.jobs.run_now(job_id=737591956135053 , 
                    job_parameters = {"CATALOG": "workspace",
                                        "SCHEMA": "default",
                                        "YEAR_START": "2009",
                                        "YEAR_END": "2013",
                                        "MONTH_START": "2009-01-01",
                                        "MONTH_END": "2013-12-31",
                                        "PROFILE": "batch_aggr",
                                        "PROFILE_DTL": "serverless_5year"
                                        }              
                    )