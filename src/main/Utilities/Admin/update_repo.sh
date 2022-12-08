#!/bin/bash

repo_url=${repo_url:-myrepo}
deployment_branch=${deployment_branch:-main}
repo_path=${repo_path:-/Repos/AzureDevops/Docker_Databricks_Connect/}

while [ $# -gt 0 ]; do

   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        # echo $1 $2 // Optional to see the parameter:value result
   fi

  shift
done

echo building from $repo_url $deployment_branch to $repo_path
databricks repos update --path $repo_path --branch $deployment_branch
