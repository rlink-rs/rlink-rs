#!/bin/bash

# envirenment list:
echo "env WORKER_PATH = $WORKER_PATH"
echo "env APPLICATION_MANAGER_ADDRESS = $APPLICATION_MANAGER_ADDRESS"
echo "env APPLICATION_ID = $APPLICATION_ID"
echo "env TASK_ID = $TASK_ID"
echo "env BIND_IP = $BIND_IP"
echo "env FILE_NAME = $FILE_NAME"
echo "env CLUSTER_CONFIG = $CLUSTER_CONFIG"
echo "env DASHBOARD_PATH = $DASHBOARD_PATH"
#echo "env ARG_KV_PAIRE = $ARG_KV_PAIRE"

#EXECUTOR_WORKSPACE=$(cd $(dirname $0); pwd)
#echo "workspace:${EXECUTOR_WORKSPACE}"

application_path=$WORKER_PATH/$APPLICATION_ID
task_workspace_path=$application_path/$TASK_ID
execute_file_path=$task_workspace_path/${FILE_NAME}

if [ ! -f "$execute_file_path" ]; then
  wget ${APPLICATION_MANAGER_ADDRESS}/job/resource/${APPLICATION_ID}/${FILE_NAME} -O $execute_file_path

  chmod +x $execute_file_path
fi

args=$@
echo "args:${args}"

nohup $execute_file_path \
  cluster_config=${CLUSTER_CONFIG} \
  application_id=${APPLICATION_ID} \
  task_id=${TASK_ID} \
  bind_ip=${BIND_IP} \
  dashboard_path=${DASHBOARD_PATH} \
  ${args} \
  1>${task_workspace_path}/stdout \
  2>${task_workspace_path}/stderr \
  & echo $!> ${task_workspace_path}/pid
