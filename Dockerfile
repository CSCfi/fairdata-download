FROM python:3.8-buster

# Configure user
ARG DEVELOPER_UID=1000
ARG DEVELOPER_GID=1000

RUN groupadd -g $DEVELOPER_GID developer
RUN useradd --create-home --no-log-init -r -u $DEVELOPER_UID \
    -g $DEVELOPER_GID developer
USER developer

ENV PATH="${PATH}:/home/developer/.local/bin"

# Install application
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt

COPY . .

# Default environment variables
ENV FLASK_APP=download
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_ENV=development

EXPOSE 5000

CMD ["flask", "run"]
