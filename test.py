# test_google_api.py

import requests

API_KEY = "AIzaSyAODD3Ns52T2bGqtGn7kvvAktXMUhi9p_s"
SEARCH_ENGINE_ID = "12c07c52e06b646c3"

response = requests.get(
    "https://www.googleapis.com/customsearch/v1",
    params={
        'key': API_KEY,
        'cx': SEARCH_ENGINE_ID,
        'q': 'Harris County TX probate court search'
    }
)

print(f"Status: {response.status_code}")
print(f"\nResults:")
for item in response.json().get('items', [])[:3]:
    print(f"  - {item['title']}")
    print(f"    {item['link']}\n")