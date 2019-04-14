#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "$0 is running from: $DIR"

# make this file's location working dir
cd "$(dirname "$0")"

mvn assembly:assembly -DdescriptorId=jar-with-dependencies
gsutil cp ./target/kafka_ingestion-1.0.0-SNAPSHOT-jar-with-dependencies.jar gs://gid-datalake-dev/kafka_ingestion-1.0.0-SNAPSHOT.jar