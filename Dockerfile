FROM python:3.9-slim-bullseye as base
ENV POETRY_HOME=/etc/poetry \
    POETRY_VERSION=1.2.1
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

FROM base AS build

RUN apt-get update && \
    apt-get install -y -q build-essential \
    curl

# For pyodbc
RUN curl -s -o microsoft.asc https://packages.microsoft.com/keys/microsoft.asc \
    && curl -s -o mssql-release.list https://packages.microsoft.com/config/debian/11/prod.list \
    && apt-get update -y \
    && apt-get install -y g++ unixodbc-dev

RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=${POETRY_HOME} python3 -
RUN poetry config virtualenvs.create false
RUN poetry config experimental.new-installer false
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc

COPY poetry.lock pyproject.toml ./
RUN poetry install --no-interaction --no-ansi --no-dev -vvv


FROM base as runtime

# For pyodbc
COPY --from=build microsoft.asc microsoft.asc
COPY --from=build mssql-release.list mssql-release.list

ENV ACCEPT_EULA=Y
RUN apt-get update -y && apt-get install -y gnupg2
RUN apt-key add microsoft.asc && rm microsoft.asc && mv mssql-release.list /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN apt-get install -y curl
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update -y
RUN apt-get install -y msodbcsql18
RUN apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
RUN apt-get install -y unixodbc-dev


RUN useradd --create-home --shell /bin/bash app
USER app

# non-interactive env vars https://bugs.launchpad.net/ubuntu/+source/ansible/+bug/1833013
ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN=true
ENV UCF_FORCE_CONFOLD=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY . ./
COPY --from=build /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

ENTRYPOINT ["bash", "start.sh"]