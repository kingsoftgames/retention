#!/bin/bash
set -x
export AWS_REGION=cn-north-1
export S3_BUCKET=rog2
export S3_KEY_PREFIX_CREATE_PLAYER=personal/jiangjhaitao/retention-test/CreatePlayer/\<yyyy\>/\<MM\>/\<d\>/
export S3_KEY_PREFIX_PLAYER_LOGIN=personal/jiangjhaitao/retention-test/EnterGame/\<yyyy\>/\<MM\>/\<d\>/
export CREATE_PLAYER_EVENT=CreatePlayer
export PLAYER_LOGIN_EVENT=EnterGame
export RETENTION_DAYS=2,3,7,14
export ES_USER=admin
export ES_PWD=admin
export ES_URL=https://52.81.20.102:9200
export ES_INDEX=retention7
export "AWS_ACCESS_KEY_ID="
export 'AWS_SECRET_ACCESS_KEY='
echo "============="
echo $AWS_ACCESS_KEY_ID
echo "============="
python retention.py -d 2019-06-29
