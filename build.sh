#!/bin/sh
set -eu

REQUIREMENTS_FILE="${SRC_PKG}/requirements-fission.txt"
if [ ! -f "${REQUIREMENTS_FILE}" ]; then
  REQUIREMENTS_FILE="${SRC_PKG}/requirements.txt"
fi

pip3 install -r "${REQUIREMENTS_FILE}" -t "${SRC_PKG}"
cp -R "${SRC_PKG}/." "${DEPLOY_PKG}/"
