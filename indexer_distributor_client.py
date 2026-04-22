import requests

DISTRIBUTOR_URL = "http://10.201.13.25:5000/add_url"

def send_links(links):
    try:
        res = requests.post(DISTRIBUTOR_URL, json={"urls": links})
        print("Distributor response:", res.json())
    except:
        print("Distributor not reachable")
