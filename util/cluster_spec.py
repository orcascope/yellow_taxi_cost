from databricks.sdk.service.compute import ClusterSpec

d4sv3_single_tot_4c_16g = ClusterSpec.from_dict({
                            "data_security_mode": "DATA_SECURITY_MODE_AUTO",
                            "custom_tags": {
                                "resource_type": "d4sv3_single_tot_4c_16g"
                            },
                            "kind": "CLASSIC_PREVIEW",
                            "azure_attributes": {
                                "availability": "SPOT_WITH_FALLBACK_AZURE"
                            },
                            "runtime_engine": "STANDARD",
                            "spark_version": "16.4.x-scala2.12",
                            "node_type_id": "Standard_D4s_v3",
                            "is_single_node": True,
                            "num_workers": 1
                        })