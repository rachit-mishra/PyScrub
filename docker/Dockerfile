# Official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set a maintainer name
LABEL maintainer="Rachit Mishra rachit.mishra94@gmail.com"

# Install Java (OpenJDK)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Dynamically set JAVA_HOME
RUN echo "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/environment

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
COPY docker/requirements.txt /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# Run app.py when the container launches
ENTRYPOINT ["python", "app.py"]
