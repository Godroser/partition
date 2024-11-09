import requests

owner = "AlibabaResearch"
repo = "DAMO-ConvAI"

url = f"https://api.ossinsight.io/v1/repos/{owner}/{repo}/issue_creators/"
headers = {
    "Accept": "application/json"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"请求失败，状态码: {response.status码}")

    