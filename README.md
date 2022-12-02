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

### Task 1

To solve this task, I chose to pre-compute the aggregated values for each sensor using common table expressions. I chose to calculate the average values of all 4 sensors per batch, but this can be easily modified to calculate other aggregated values like max, sum and etc. 
I assumed that we are interested only in the productions batches, and thus did not included any of the test_batches. Since we are interested only for the cultivation phase, I joined the `batch_info` and `batch_phase` using the criterion that the `batch_phase.start_date` should lie between the start_date and end_date of each batch. Moreover, I joined the table of each sensor, with the criterion that the sensor timestamp should lie between the start_date and end_date of the batch phase, keeping only the cultivation phase of each batch.
After computing the 4 tables using CTE, those tables are joined on batch_id to add the aggregated value of each sensor as a column and produce the final table. The table is stored as CSV in `results/table`.

### Task 2
For this task I created 2 subplots for each batch_id (skipping again the test batches) showing the difference temperature 2 - 1 and ph 2 - 1 over time. 
Again, I joined batch_info and batch_phase tables in a similar manner as in task 1 to keep only the cultivation phase. Then I joined the sensor 2 table on the criterion that the timestamp should lie between the batch_phase start and end date. Since the timestamp of the 2 sensors do not much exactly, I couldn't join the sensor 1 table on the exact timestamp, and thus I joined it on the datetime, excluding the seconds. 
One thing I noticed when doing this, was that sensor timestamp 1 was before sensor timestamp 2, and thus decided that the join should happen on the timestamps truncated to the minute, but also rounded up (addind 30 seconds did the trick).
Below there is one example showing the issue:

Without the rounding trick:
![image](https://user-images.githubusercontent.com/119599398/205278905-6e5e6b84-ba2c-463a-9018-0f0de1a05e33.png)

With the rounding trick:
![image](https://user-images.githubusercontent.com/119599398/205279093-97fada03-ab55-418e-ac0f-48e48f517d15.png)

The data is calculated in a table for each batch, and the plots are being created using Plotly library. The plots can be found in `results/plots`

