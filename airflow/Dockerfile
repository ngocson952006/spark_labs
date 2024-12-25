# Start with a slim base and manually install Java
FROM openjdk:11-jdk-slim AS java_base

# Use Java-enabled base and install Airflow dependencies
FROM apache/airflow:2.10.4
COPY --from=java_base /usr/local/openjdk-11 /usr/local/openjdk-11

# Set required environment variables
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH

# Install additional packages if required
USER root
RUN apt-get update && apt-get install -y software-properties-common && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow