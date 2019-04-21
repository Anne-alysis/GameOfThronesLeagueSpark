#!/usr/bin/env bash
bucket=SCRUBBED
path=SCRUBBED
jarname=SCRUBBED
jar=SCRUBBED
localpath=SCRUBBED

week=1
createquestionstructure=false

cluster=SCRUBBED
class=com.github.annealysis.gameofthrones.Score

# clean up old jars
set +e; gsutil -m rm -rf ${path}/${jarname}; set -e; sleep 1

# push our new jar
gsutil -m cp ${localpath}/target/${jarname} ${jar}
sleep 1

gcloud dataproc jobs submit spark \
    --cluster=${cluster} \
    --region=us-central1 \
    --bucket=${bucket} \
    --jars=${jar} \
    --class=${class} \
    -- ${path} \
    ${week} \
    ${createquestionstructure}

