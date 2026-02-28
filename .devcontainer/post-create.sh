#!/usr/bin/env bash
set -euo pipefail

workspace_dir="${1:-$PWD}"
export DEBIAN_FRONTEND=noninteractive

# ensure safe home for codex
mkdir -p "$workspace_dir/.codex-home"
rm -rf "$HOME/.codex"
ln -s "$workspace_dir/.codex-home" "$HOME/.codex"

# update and install required system packages
sudo apt update
sudo apt upgrade -y
sudo apt install -y docker.io python3-pip python3-venv

# create and populate python environment
python3 -m venv "$workspace_dir/.venv"
source "$workspace_dir/.venv/bin/activate"
pip install --no-cache-dir dbt-postgres requests sqlalchemy psycopg2-binary pandas pyarrow