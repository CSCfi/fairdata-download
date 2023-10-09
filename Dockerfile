FROM python:3.8-buster

# Install application
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt

COPY . .

# Configure volumes
RUN mkdir /mnt/download-ida-storage
RUN mkdir /mnt/download-service-cache

VOLUME /mnt/download-ida-storage
VOLUME /mnt/download-service-cache

# Default environment variables
ENV FLASK_APP=download
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_DEBUG=1

EXPOSE 5000

CMD ["flask", "run"]
