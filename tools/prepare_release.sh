#!/bin/bash

set -euo pipefail

VERSION=${1-}
REQUIRED_TOOLS="sbt git"

if test -z "$VERSION"; then
  echo "Missing version parameter. Usage: $0 VERSION"
  exit 1
fi

for tool in $REQUIRED_TOOLS; do
  if ! hash "$tool" 2>/dev/null; then
    echo "This script requires '$tool', but it is not installed."
    exit 1
  fi
done

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "Cannot prepare release, a release for $VERSION already exists"
  exit 1
fi

sed -i '' "s/^spark-shell --jars spark-connector-assembly-.*/spark-shell --jars spark-connector-assembly-$VERSION.jar/" README.md
sed -i '' "s/^\`.\/target\/scala-2.12\/spark-connector-assembly-.*/\`.\/target\/scala-2.12\/spark-connector-assembly-$VERSION.jar/" README.md

git commit -a -m "Setting version in README.md to $VERSION"

sbt "release with-defaults release-version $VERSION"
