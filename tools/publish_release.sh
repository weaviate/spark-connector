#!/bin/bash

set -euo pipefail

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path/.."

# decrypt sonatype.sbt + passphrase.env + key.gpg
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_PASSPHRASE" \
--output .secrets.tar .secrets.tar.gpg
tar xvf .secrets.tar

# import key
gpg --batch --import key.gpg

# load passphrase
source ./passphrase.env

# sign artifact
sbt +publishSigned

# upload artifact to sonatype (to be replaced with 'sonatypeBundleRelease' later)
sbt +sonatypePrepare
sbt sonatypeBundleUpload

# upload artifact to sonatype and release
sbt sonatypeBundleRelease
