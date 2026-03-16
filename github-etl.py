import requests
import pandas as pd

repo_owner = "tensorflow"
repo_name = "tensorflow"
url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/commits?per_page=50"

response = requests.get(url)
data = response.json()

commits = []
for commit in data:
    commits.append({
        "sha": commit.get("sha"),
        "author": commit["commit"]["author"].get("name"),
        "date": commit["commit"]["author"].get("date"),
        "message": commit["commit"].get("message")
    })

df = pd.DataFrame(commits)
df['date'] = pd.to_datetime(df['date'])
print(df.head())
df.to_csv("github_commits.csv", index=False)

