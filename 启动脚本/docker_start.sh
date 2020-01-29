#!/bin/bash

echo "--------------DOCKER_START---------------"
systemctl start docker
echo "--------------MYSQL_START----------------"
docker start ba209e8ab8afaf1c88e567bd1f8a85ae6d4517a558cf37127947809ce6c00b4e