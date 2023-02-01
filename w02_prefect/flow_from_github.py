from prefect.filesystems import GitHub

github = GitHub(
    access_token="YOUR_GITHUB_ACCESS_TOKEN", 
    repo="owner/repo"
    )

flow_file = "path/to/flow.py"
flow_content = github.read(flow_file)


# To get a GitHub access token, 
# you need to have a GitHub account and follow these steps:

# - Go to the GitHub Developer Settings page (https://github.com/settings/developers)
# - Click on the "Personal access tokens" section.
# - Click the "Generate new token" button.
# - Provide a descriptive name for your token, 
#   select the scope of access you need, and click the "Generate token" button