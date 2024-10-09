#!/bin/bash

# Default values
mode="oss"
repo_url="https://github.com/hatchet-dev/hatchet.git"
submodule_name="hatchet"

# Parse command line options
while getopts ":n:" opt; do
  case $opt in
    n)
      mode=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

# Set the repository URL and submodule name based on the mode
if [ "$mode" = "cloud" ]; then
  repo_url="https://github.com/hatchet-dev/hatchet-cloud.git"
  submodule_name="hatchet-cloud"
else
  repo_url="https://github.com/hatchet-dev/hatchet.git"
  submodule_name="hatchet"
fi

echo "Mode: $mode"
echo "Repository URL: $repo_url"
echo "Submodule name: $submodule_name"

# 1. Get the current branch name
current_branch=$(echo $GITHUB_HEAD_REF | sed 's/refs\/heads\///')

if [ -z "$current_branch" ]; then
    current_branch=$(git rev-parse --abbrev-ref HEAD)
fi

echo "Current branch: $current_branch"

# 2. Check the repo and determine if a branch with the same name exists
git ls-remote --heads $repo_url $current_branch | grep -q refs/heads/$current_branch
branch_exists=$?

# 3. If it does, update the .gitmodules to set `branch = {the branch name}`
if [ $branch_exists -eq 0 ]; then
    git config -f .gitmodules submodule.$submodule_name.branch $current_branch
    git config -f .gitmodules submodule.$submodule_name.path $submodule_name
    git config -f .gitmodules submodule.$submodule_name.url $repo_url
    git add .gitmodules
    echo "Updated .gitmodules with branch $current_branch"
else
    echo "Branch $current_branch does not exist in the remote repository. Using main branch instead."
    git config -f .gitmodules submodule.$submodule_name.branch main
    git config -f .gitmodules submodule.$submodule_name.path $submodule_name
    git config -f .gitmodules submodule.$submodule_name.url $repo_url
    git add .gitmodules
    echo "Updated .gitmodules with branch main"
fi

# 4. Remove existing submodule if it exists
git submodule deinit -f -- $submodule_name
rm -rf .git/modules/$submodule_name
git rm -f $submodule_name

# 5. Re-add and initialize the submodule
git submodule add -b $(git config -f .gitmodules submodule.$submodule_name.branch) $repo_url $submodule_name
git submodule init

# 6. Update the submodule
git submodule update --remote --merge

echo "Hatchet submodule ($mode mode) updated successfully"