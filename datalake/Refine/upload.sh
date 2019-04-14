#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "$0 is running from: $DIR"

# make this file's location working dir
cd "$(dirname "$0")"

gsutil cp ./target/refine-1.0.0-SNAPSHOT-jar-with-dependencies.jar gs://gid-datalake-dev/refine-1.0.0-SNAPSHOT.jar