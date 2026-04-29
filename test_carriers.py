import os
import requests
from dotenv import load_dotenv

load_dotenv()

script_url = os.getenv('GOOGLE_SCRIPT_URL')

print(f"URL: {script_url}")

payload = {'action': 'list_carriers'}

response = requests.post(script_url, json=payload, timeout=10)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")

if response.status_code == 200:
    data = response.json()
    print(f"Success: {data.get('success')}")
    print(f"Carriers: {data.get('carriers')}")
