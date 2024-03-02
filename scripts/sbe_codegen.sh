#!/bin/bash

# any version changes here should also be bumped in Dockerfile.buf
BUF_VERSION='1.0.0-rc12'
PROTOC_GEN_GO_VERSION='v1.27.1'
PROTOC_GEN_GO_GRPC_VERSION='1.2.0'

if ! [[ "$0" =~ scripts/sbe_codegen.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

echo "generating Simple Binary Encodings"

OUT_DIR=sbe
FLAGS="-Dsbe.generate.ir=true -Dsbe.target.language=Golang -Dsbe.target.namespace=sbe -Dsbe.output.dir=$OUT_DIR -Dsbe.xinclude.aware=true -Dsbe.errorLog=yes"
CMD="java $FLAGS -jar ./sbe-schema/sbe-all-1.31.0-SNAPSHOT.jar sbe-schema/ftso-data-sources-schema.xml"

$CMD