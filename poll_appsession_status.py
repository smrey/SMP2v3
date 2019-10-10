import requests
import time
from config import v1_api
from config import v2_api


class PollAppsessionStatus:

    def __init__(self, auth, appsession_id):
        self.authorise = auth
        self.appsession_id = appsession_id
        self.sleep_time = 900 # 15 minutes in seconds


    def poll(self):
        url = f"{v2_api}/appsessions/{self.appsession_id}"
        head = {"Authorization": self.authorise, "User-Agent": "/python-requests/2.22.0"}
        response = requests.get(url, headers=head, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            if response.json().get("ExecutionStatus") == "Complete":
                return "Complete"
            elif response.json().get("ExecutionStatus") == "Aborted":
                return "Fail"
            time.sleep(self.sleep_time)
        return self.poll()


    def find_appresults(self):
        url = f"{v1_api}/appsessions/{self.appsession_id}/appresults"
        p = {"Limit": 40}
        head = {"Authorization": self.authorise, "User-Agent": "/python-requests/2.22.0"}
        response = requests.get(url, headers=head, params=p, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return {items.get("Name"): items.get("Id") for items in response.json().get("Response").get("Items")}
