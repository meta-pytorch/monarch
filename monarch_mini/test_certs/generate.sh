#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

set -eu

cert_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
umask 077

openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -keyout "$tmp_dir/ca-key.pem" \
  -out "$cert_dir/ca.pem" \
  -days 3650 \
  -subj "/CN=Minimonarch Test CA"

openssl req \
  -new \
  -newkey rsa:2048 \
  -nodes \
  -keyout "$cert_dir/key.pem" \
  -out "$tmp_dir/server.csr" \
  -subj "/CN=monarch-mini"

printf '%s\n' \
  'subjectAltName=DNS:monarch-mini,DNS:localhost,IP:127.0.0.1,IP:::1' \
  'extendedKeyUsage=serverAuth' \
  >"$tmp_dir/server.ext"

openssl x509 \
  -req \
  -in "$tmp_dir/server.csr" \
  -CA "$cert_dir/ca.pem" \
  -CAkey "$tmp_dir/ca-key.pem" \
  -CAserial "$tmp_dir/ca.srl" \
  -CAcreateserial \
  -out "$cert_dir/cert.pem" \
  -days 3650 \
  -extfile "$tmp_dir/server.ext"

chmod 644 "$cert_dir/ca.pem" "$cert_dir/cert.pem"
chmod 600 "$cert_dir/key.pem"
