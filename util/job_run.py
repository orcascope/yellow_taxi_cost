import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# ddl_setup_response = w.jobs.run_now(job_id=119088678823965 , 
#                     job_parameters = {
#                         "CATALOG": "workspace",
#                         "SCHEMA": "default",
#                         "PROFILE": "batch_aggr",
#                         "PROFILE_DTL": "d4sv3_1w_tot_8c_32g_5year"
#                     })

daily_load_response = w.jobs.run_now(job_id=1034890538031766 , 
                    job_parameters = {"CATALOG": "workspace",
                                        "SCHEMA": "default",
                                        "YEAR_START": "2014",
                                        "YEAR_END": "2014",
                                        "MONTH_START": "2014-01-01",
                                        "MONTH_END": "2014-12-31",
                                        "PROFILE": "batch_aggr",
                                        "PROFILE_DTL": "d4sv3_1w_tot_8c_32g_2014"
                                        }              
                    )




#- end of script                    