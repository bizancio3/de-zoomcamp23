# Recipe reference from prefect docs:
# https://towardsdatascience.com/create-robust-data-pipelines-with-prefect-docker-and-github-12b231ca6ed2

""" example running from command line:

prefect deployment build src/main.py:create_pytrends_report \
  -n google-trends-gh-docker \          #name
  -q test \                             #queue name
  -sb github/pytrends \                 #storage block with path to repo 
  -ib docker-container/google-trends \  #docker block (deployed artifact)
  -o prefect-docker-deployment \        #outputs .yaml file
  --apply

"""

# or embedded in python script

from prefect.filesystems import GitHub

github = GitHub(access_token="YOUR_GITHUB_ACCESS_TOKEN", repo="owner/repo")

flow_file = "path/to/flow.py"
flow_content = github.read(flow_file)


""" 
Steps for getting a GitHub access token:

- Go to the GitHub Developer Settings page (https://github.com/settings/developers)
- Click on the "Personal access tokens" section.
- Click the "Generate new token" button.
- Provide a descriptive name for your token, 
select the scope of access you need, and click the "Generate token" button
"""
