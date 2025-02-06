# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.6.0

# Switch to the root user to install additional packages
USER root

# Update package lists, install pip for Python 3, and clean up apt cache
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for security reasons
USER airflow

# Copy the DAGs (Directed Acyclic Graphs) from the local directory to the container
COPY dags/ /opt/airflow/dags/

# Copy the requirements.txt file containing necessary Python dependencies
COPY requirements.txt /opt/airflow/

# Install the required Python packages from the requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
