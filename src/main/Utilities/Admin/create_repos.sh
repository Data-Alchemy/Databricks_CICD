#!/bin/bash
: '  
Provider options are:
--------------------
azureDevOpsServices
bitbucketCloud
bitbucketServer
gitHub
gitHubEnterprise
gitLab
gitLabEnterpriseEdition
'  

repo_url=${repo_url:-myrepo}
provider=${provider:azureDevOpsServices}
repo_path=${repo_path:-/Repos/AzureDevops/Databricks_CICD/}

while [ $# -gt 0 ]; do

   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        # echo $1 $2 // Optional to see the parameter:value result
   fi

  shift
done

databricks repos create --url $repo_url --provider $provider --path $repo_path