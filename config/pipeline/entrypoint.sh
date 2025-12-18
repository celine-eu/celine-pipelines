#!/usr/bin/env bash
set -euo pipefail


if [ "$PREFECT_MODE" = "dev" ]; then

  echo "Starting in dev mode"

  PIPELINES_ROOT=${PIPELINES_ROOT:-/pipelines}
  REINSTALL=${REINSTALL:-false}  # Set to true to force reinstall

  DEPS_DIR="/deps"
  BASE_DIR="$PIPELINES_ROOT/apps"

  APP_NAME=${APP_NAME:?APP_NAME env var required}
  APP_PATH="$BASE_DIR/$APP_NAME"

  if [ ! -d "$APP_PATH" ]; then
    echo "ERROR: $APP_PATH does not exist"
    exit 1
  fi

  echo "Setting up: $APP_NAME"

  export MELTANO_PROJECT_ROOT="${APP_PATH}/meltano"
  export DBT_PROJECT_DIR="${APP_PATH}/dbt"
  export DBT_PROFILES_DIR="${DBT_PROJECT_DIR}"

  # Check if $APP_PATH/requirements.txt exists
  if [ -f "$APP_PATH/requirements.txt" ]; then
      echo "Found $APP_PATH/requirements.txt. Installing local dependencies..."
      uv pip install -r "$APP_PATH/requirements.txt"
  else
      echo "No $APP_PATH/requirements.txt found. Skipping local dependencies."
  fi

  # Install Python dependencies in deps/
  if [ -d "$DEPS_DIR" ]; then
    for dep in "$DEPS_DIR"/*; do
      [ -d "$dep" ] || continue
      pkg_name=$(basename "$dep")

      if [ "$REINSTALL" = "true" ]; then
        echo "Reinstalling dep: $pkg_name"
        pip install -e "$dep"
      else
        if ! pip show "$pkg_name" >/dev/null 2>&1; then
          echo "Installing dep: $pkg_name"
          pip install -e "$dep"
        fi
      fi
    done
  fi

  # Meltano setup
  if [ -d "$APP_PATH/meltano" ]; then
    cd "$APP_PATH/meltano"

    if [ "$REINSTALL" = "true" ] && [ -d ".meltano" ]; then
      echo "  -> Removing existing .meltano folder"
      rm -rf .meltano
    fi

    if [ "$REINSTALL" = "true" ] || [ ! -d ".meltano" ]; then
      echo "  -> Installing Meltano plugins"
      MELTANO_PROJECT_ROOT=$(pwd) meltano install
    else
      echo "  -> Meltano plugins already installed, skipping..."
    fi
  fi

  # dbt setup
  if [ -d "$APP_PATH/dbt" ]; then
    cd "$APP_PATH/dbt"

    if [ "$REINSTALL" = "true" ]; then
      echo "  -> Removing existing dbt target, dbt_packages, and .dbt folders"
      rm -rf target dbt_packages .dbt
    fi

    if [ "$REINSTALL" = "true" ] || [ ! -d "dbt_packages" ]; then
      echo "  -> Installing dbt dependencies"
      DBT_PROFILES_DIR=$(pwd) dbt deps
    else
      echo "  -> dbt dependencies already installed, skipping..."
    fi
  fi

  cd $APP_PATH

fi

exec "$@"
