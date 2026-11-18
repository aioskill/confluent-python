#!/bin/bash 

export ENV_ID=env-qqoypp
export CLUSTER_ID=lkc-6k6xqxj
export SERVICE_ACCOUNT_ID=sa-5m31kw8


confluent api-key create \
  --resource $CLUSTER_ID \
  --service-account $SERVICE_ACCOUNT_ID \
  --description "API key for $ENV_ID" \
  --output json



 confluent iam rbac role-binding create \
  --principal User:$SERVICE_ACCOUNT_ID \
  --role DeveloperRead \
  --environment $ENV_ID \
  --cloud-cluster $CLUSTER_ID \
  --kafka-cluster $CLUSTER_ID \
  --resource Topic:cli-demo* 


 confluent iam rbac role-binding create \
  --principal User:$SERVICE_ACCOUNT_ID \
  --role DeveloperWrite \
  --environment $ENV_ID \
  --cloud-cluster $CLUSTER_ID \
  --kafka-cluster $CLUSTER_ID \
  --resource Topic:cli-demo* 