
ARG BASE_TAG=latest
FROM ghcr.io/celine-eu/pipeline:${BASE_TAG}

# Build arg for app name
ARG APP_NAME
ENV APP_NAME=${APP_NAME}
ENV OPENLINEAGE_NAMESPACE=${APP_NAME}

ENV PIPELINES_ROOT=${PIPELINES_ROOT:-/pipelines}
ENV BASE_DIR="${PIPELINES_ROOT}/apps"

ENV APP_PATH="${BASE_DIR}/${APP_NAME}"

ENV MELTANO_PROJECT_ROOT="${APP_PATH}/meltano"

ENV DBT_PROJECT_DIR="${APP_PATH}/dbt"
ENV DBT_PROFILES_DIR="${DBT_PROJECT_DIR}"

ENV PATH="/pipelines/.venv/bin:$PATH"

WORKDIR /pipelines

# Copy only the selected app (not all apps)
COPY ./apps/${APP_NAME} /pipelines/apps/${APP_NAME}

RUN uv sync

# Install app dependencies, venv is /pipelines/.venv
RUN if [ -f "${APP_PATH}/requirements.txt" ]; then \
    uv pip install -r "${APP_PATH}/requirements.txt"; \
    fi

# install meltano deps
RUN if [ -d "${MELTANO_PROJECT_ROOT}" ]; then \
    cd "${MELTANO_PROJECT_ROOT}" && \
    rm -rf .meltano && \
    MELTANO_PROJECT_ROOT=$(pwd) meltano install ; \
    fi

# install dbt deps
RUN if [ -d "${DBT_PROJECT_DIR}" ]; then \
    cd "${DBT_PROJECT_DIR}" && \
    rm -rf target dbt_packages .dbt && \
    DBT_PROFILES_DIR=$(pwd) dbt deps ; \
    fi

# Switch into app dir
WORKDIR ${APP_PATH}
