#!/bin/bash

# Stop Dataverse Connector Demo Environment
# This script stops the Docker environment and optionally cleans up

read -p "Do you want to remove all containers and volumes? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Stopping and removing all containers and volumes..."
    docker compose down -v
    echo "Done! All containers and volumes have been removed."
else
    echo "Stopping containers (keeping volumes)..."
    docker compose down
    echo "Done! Containers have been stopped but volumes are preserved."
    echo "To start the environment again, run: ./start-environment.sh"
fi 