import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

def get_applications(authorise):
    file_name = os.path.basename(file_to_upload)
    print(file_name)
    url = v2_api + "/appresults/" + appresult_id + "/files"
    p = {"name": "reportarchive.zip", "multipart":"true"}
    print(url)
    data = {"Content-Type": "application/zip", "Name": file_name}
    head = {"Authorization": authorise}
    response = requests.post(url, params=p, data=data, headers=head,
                             allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        print(response.json())
    return None




def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")  # TODO This could be better as a global variable?


if __name__ == '__main__':
    main()