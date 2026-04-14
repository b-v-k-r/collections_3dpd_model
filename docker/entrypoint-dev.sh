#!/usr/bin/env bash
# Define the private repository name
REPO_NAME="collections_2dpd_model"
NAME_SPACE="stage-dataplatform"
BRANCH="main"

echo "Pulling secrets from vault!"
#Set up the SSH key from the Kubernetes secret
mkdir -p ~/.ssh

kubectl get secret/airflow-ssh-git-secret -o=jsonpath='{.data.gitSshKey}' -n "$NAME_SPACE" | base64 -d > ~/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa

echo "SSH key set up successfully!"

# # Clone the private Git repository (replace the repository URL and path as needed)
# # Disable strict host key checking and clone the private Git repository
if [ -d "/app/$REPO_NAME/.git" ]; then
    echo "repo already present, skipping clone"
else
    GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no" git clone -b "$BRANCH" git@github.com:khatabook/"$REPO_NAME".git /app/"$REPO_NAME"
fi

echo "repo cloned successfully!"
