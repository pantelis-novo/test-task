# Task solution for the backend developer at Novo Nordisk

### Author: Pantelis Kouris

## Requirements

To run the scripts, you will need to install docker, docker-compose and git.

## Instructions

  1. Clone this repo `git clone https://github.com/pantelis-novo/test-task`
  2. Change directory `cd test-task/deploy`
  3. Run `docker-compose -up --build --force-recreate --no-deps`
  4. To shutdown the running database container `docker-compose down`
  
  
## A few words about the code

  * A PostgreSQL database is being created to store and query the data.
  * SQLAlchemy ORM is used for interaction between the database and Python
  * In the `src` directory there are 3 main scripts.
      * `ingest_data.py` For parsing the CSV files and inserting the data into the database instance
      * `generate_aggregated_table.py` For querying the database and generating a table with aggregated information about all batches
      * `plot_sensor_difference.py` For computing and visualizing the difference between temperature and ph sensors for the cultivation phase
  * A `Dockerfile` is provided for installing the required Python dependencies and running the scripts
  * Additionally there is one `database.py` file, which includes the SQLAlchemy Declarative Mapping for interacting with the actual tables of the database as well as helper functions for querying the data from the database.
  * In the `deploy` folder you will find
    * A `postgres_db` folder which includes the Dockerfile for running and setting up a PostgreSQL 15 image
    * A `docker-compose.yml` that combines the PostgreSQL and scripts images. This docker-compose ensures that the database is running, before executing the scripts
## A few words about the solution

  *
