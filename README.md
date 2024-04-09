# PyScrub
Welcome to PyScrub! Leveraging the robustness of PySpark and the versatility of Docker, PyScrub stands as a premier solution for cleaning and profiling large datasets. Designed to tackle a wide array of data issues—from inconsistencies and missing values to comprehensive data quality assessment—PyScrub ensures your data is not just clean, but also understood.

## Purpose
The essence of PyScrub is to offer a scalable, user-friendly platform for data scientists and analysts to enhance their data quality with minimal effort. It's built with the vision of handling vast datasets, providing detailed insights into your data, and preparing it for further analysis or machine learning processes. Whether you are looking to preprocess data for analytics or ensure data quality in your data pipelines, PyScrub provides the tools you need.

## Getting Started
To get started with PyScrub, follow these simple steps:

## Prerequisites
Docker installed on your system
Basic understanding of Docker commands
Familiarity with JSON format for configuration

## Installation and Setup

1. Clone the Repository : Start by cloning the PyScrub repository to your local machine:

```sh
git clone https://github.com/<your-username>/PyScrub.git
cd PyScrub
```
Replace <your-username> with your GitHub username or the username of the repository's owner.

2. Build the Docker Image : Inside the PyScrub directory, build the Docker image by executing:

```sh
docker build -t pyscrub -f docker/Dockerfile .
```
This command constructs a Docker image named pyscrub, based on the Dockerfile located in the docker/ directory.

3. Prepare the Configuration File : PyScrub uses a config.json file to specify data cleaning and profiling settings. Create your configuration file by copying the provided template. 

```sh
cp config_template.json config.json
```
After copying, open config.json with your preferred text editor and adjust the settings to match your dataset and cleaning/profiling requirements.

4. Running PyScrub : Once the Docker image is built and your config.json is configured, run PyScrub with the following command.

```sh
docker run -it --rm --name pyscrub-instance -v $(pwd)/data:/app/data pyscrub python app.py config.json
```
This command mounts your local data directory into /app/data inside the Docker container and starts PyScrub using your configuration file. Place your data file in folder /data/ (currently holds a dummy_data.csv)

## Contributing to PyScrub
We welcome contributions! Whether it's adding new features, improving documentation, or reporting bugs, your help makes PyScrub better for everyone. Before contributing, please review our contributing guidelines in CONTRIBUTING.md.

## Getting Help
If you encounter any issues or have questions about PyScrub, feel free to open an issue on GitHub. For more detailed inquiries, you can contact us directly at [rachit.mishra94@gmail.com].
