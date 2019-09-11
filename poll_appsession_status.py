import requests
import time
from config import v1_api
from config import v2_api

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

class PollAppsessionStatus:

    def __init__(self, auth, appsession_id):
        self.authorise = auth
        self.appsession_id = appsession_id
        self.sleep_time = 1800 # Half an hour in seconds #TODO adjust for app runtime


    def poll(self):
        url = f"{v2_api}/appsessions/{self.appsession_id}"
        head = {"Authorization": self.authorise, "User-Agent": "/python-requests/2.22.0"}
        response = requests.get(url, headers=head, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            if response.json().get("ExecutionStatus") == "Complete":
                return "All appsessions complete"
            time.sleep(self.sleep_time)
        return self.poll()


    def find_appresults(self):
        url = f"{v1_api}/appsessions/{self.appsession_id}/appresults"
        p = {"Limit": 30}
        head = {"Authorization": self.authorise, "User-Agent": "/python-requests/2.22.0"}
        response = requests.get(url, headers=head, params=p, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        print(response.json().get("Response").get("Items"))
        return {items.get("Name"): items.get("Id") for items in response.json().get("Response").get("Items")}
