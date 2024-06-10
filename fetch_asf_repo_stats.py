import requests
import time
import csv

# Your GitHub personal access token
GITHUB_TOKEN = ''

# GraphQL endpoint
GRAPHQL_URL = "https://api.github.com/graphql"

# GraphQL query to fetch repository data
def fetch_repo_data(cursor=None):
    query = """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 100, after: $cursor) {
          pageInfo {
            endCursor
            hasNextPage
          }
          nodes {
            name
            forks {
              totalCount
            }
            stargazers {
              totalCount
            }
            issues(states: OPEN) {
              totalCount
            }
            watchers {
              totalCount
            }
            pullRequests(states: OPEN) {
              totalCount
            }
            defaultBranchRef {
              target {
                ... on Commit {
                  history {
                    totalCount
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    variables = {
        "org": "apache",
        "cursor": cursor
    }
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}"
    }
    response = requests.post(GRAPHQL_URL, json={'query': query, 'variables': variables}, headers=headers)
    return response.json()

# Function to safely fetch data and handle errors with retries
def safe_fetch_repo_data(cursor=None, retries=3, delay=10):
    while retries > 0:
        try:
            result = fetch_repo_data(cursor)
            if 'errors' in result:
                print(f"Error in API response: {result['errors']}")
                time.sleep(delay)  # Wait before retrying
                retries -= 1
                continue
            return result
        except Exception as e:
            print(f"Exception occurred: {e}")
            time.sleep(delay)  # Wait before retrying
            retries -= 1
    print("Failed to fetch data after multiple retries.")
    return None

# Initialize an empty list to hold all repositories
all_repos = []

# Fetch all repositories with pagination
cursor = None
while True:
    result = safe_fetch_repo_data(cursor)
    if result is None:
        # Skip the current cursor and continue with the next one
        cursor = None
        continue
    data = result.get('data', {}).get('organization', {}).get('repositories', {})
    if not data:
        print("No data found in the response.")
        break
    for repo in data.get('nodes', []):
        if repo['forks']['totalCount'] > 0 and repo['stargazers']['totalCount'] > 0 and repo['issues']['totalCount'] > 0 and repo['watchers']['totalCount'] > 0:
            repo_data = {
                'name': repo['name'],
                'forks_count': repo['forks']['totalCount'],
                'stargazers_count': repo['stargazers']['totalCount'],
                'open_issues_count': repo['issues']['totalCount'],
                'watchers_count': repo['watchers']['totalCount'],
                'commits_count': repo['defaultBranchRef']['target']['history']['totalCount'] if repo['defaultBranchRef'] and repo['defaultBranchRef']['target'] else 0,
                'open_prs_count': repo['pullRequests']['totalCount']
            }
            all_repos.append(repo_data)
            # Log the stats for the repository
            print(f"Fetched stats for repository {repo_data['name']}: Forks={repo_data['forks_count']}, "
                  f"Stars={repo_data['stargazers_count']}, Issues={repo_data['open_issues_count']}, "
                  f"Watchers={repo_data['watchers_count']}, Commits={repo_data['commits_count']}, "
                  f"Open PRs={repo_data['open_prs_count']}")
    if not data['pageInfo']['hasNextPage']:
        break
    cursor = data['pageInfo']['endCursor']
    time.sleep(3)  # Wait for 3 seconds before the next request

# Sort repositories by forks_count in descending order
sorted_repos = sorted(all_repos, key=lambda x: x['forks_count'], reverse=True)

# Write the sorted repositories to a CSV file
with open('apache_repos.csv', 'w', newline='') as csvfile:
    fieldnames = ['name', 'forks_count', 'stargazers_count', 'open_issues_count', 'watchers_count', 'commits_count', 'open_prs_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for repo in sorted_repos:
        writer.writerow(repo)

print("CSV file 'apache_repos.csv' has been created successfully.")
