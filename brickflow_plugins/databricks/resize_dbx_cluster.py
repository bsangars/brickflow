import logging as log
import requests
import json

try:
    from brickflow import ctx
except ImportError:
    raise ImportError(
        "plugin requires brickflow context , please install library at cluster/workflow/task level"
    )


class ResizeDatabricksCluster:
    """Class that resizes a Databricks cluster"""

    def __init__(self, workers, secret_scope, host_name):
        """Class Parameters

        Args:
            workers (integer)): Number of of workers to resize the cluster to
            secret_scope (string): Databricks secret scope containing the token to authenticate the API request
            host_name (string): Databricks host name without the protocol
        """
        self.workers = workers
        self.secret_scope = secret_scope
        self.host_name = host_name
        self.log = log

    def _get_host_details(self):
        _spark = ctx.spark
        self.cluster_id = _spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        self.token = ctx.dbutils.secrets.get(self.secret_scope, "token")
        return self

    def resize(self):
        try:
            self._get_host_details()
            url = f"https://{self.host_name}/api/2.0/clusters/resize"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
            payload = {"cluster_id": self.cluster_id, "num_workers": self.workers}
            response = requests.post(url, headers=headers, data=json.dumps(payload))

            if response.status_code == 200:
                self.log.info("Cluster resized successfully")
                print("Cluster resized successfully")
            else:
                print(f"Error: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
