# PyScrub
Built on the powerful PySpark framework and packaged within a Docker environment, PyWash offers a scalable, flexible, and efficient way to cleanse and profile large datasets. Whether you're dealing with inconsistencies, missing values, or you need to understand your data's structure and quality at a glance, PyWash is here to help.

Run docker build : docker build -t pywash -f docker/Dockerfile .

Run docker run -it --rm --name pyscrub-instance -v $(pwd)/data:/app/data pyscrub

## Configuration

To configure PyWash for your datasets, duplicate the `config_template.json` file and rename the copy to `config.json`. Adjust the settings in `config.json` to match your dataset and cleaning/profiling requirements.

Example:

```bash
cp config_template.json config.json
# Edit config.json as needed