import requests

url = "https://www.nseindia.com/api/allIndices"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
}

response = requests.get(url, headers=headers, timeout=15)

print("STATUS:", response.status_code)

data = response.json().get("data", [])

print("TOTAL INDEX RECORDS:", len(data))

print("\nFIRST 10 INDEX RECORDS:\n")

for item in data[:10]:
    print(item)