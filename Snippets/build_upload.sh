#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "$0 is running from: $DIR"

# make this file's location working dir
cd "$(dirname "$0")"

mvn assembly:assembly -DdescriptorId=jar-with-dependencies
gsutil cp ./target/Snippets-1.0.0-SNAPSHOT-jar-with-dependencies.jar gs://gid-ram/Snippets-1.0.0-SNAPSHOT.jar