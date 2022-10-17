# Sparkify orchestrates with Airflow

Sparkify, a startup with a new streaming app, has recently seen an increase in its customer base and its song database system. After discussions with an IT consulting firm, the company has decided to use Airflow to orchestrated their ETL (Extract, Transform and Load) processess. Sparkify also has decided to not only move its system operations to the cloud, but also replace its small database with AWS Redshift, a fully managed, scalable data warehouse service from Amazon.


### Table of Contents

- [Project Summary](#project)
- [Data pipelines](#pipelines)
- [Airflow and Python](#airflow)
- [How to run the Python Scripts](#python)
- [The ETL Process](#etl)
- [Sparkify Data Files](#files)
- [Resulting ER Diagram](#result)


<a name="project"/>

## Project Summary


After a steady growth of its user base, Sparkify decided to move its operations to the cloud. The company chose AWS as its cloud provider and currently uses S3 as its central repository for storing all structured and unstructured data. Currently, all files are stored in JSON format. To extract, load and transform the data, Sparkify will use Redshift as its new data warehouse solution. Once all the data is collected, it will be extracted from S3 and staged in Redshift, where it will be transformed, derived, and finally uploaded to the data warehouse tables. Since most of the company's data engineers are proficient in Python, they decided to use Airflow to create, schedule and monitor their data pipelines. Below is an image of a set of unrelated tasks of a pipeline in Airflow, i.e. it shows all the necessary tasks to move data from a source (or multiple sources) to a database or data warehouse solution like AWS Redshift. Once the dependencies are created, this pipeline becomes a DAG (Directed Acyclic Graph). See section "The ETL Process".

<p align="center">
<img  width="100%" height= "100%" src=images/airflow_graph_ui.png alt="Full Airflow Diagram">
</p>


<a name="pipelines"/>

## Data pipelines

Data pipelines generally consist of a sequence of tasks that move and unify data from a growing number of disparate sources and formats to a single or multiple targets for analytics and business intelligence. In Sparkify's case, the first step in our pipeline is to extract all the necessary data about artists, songs, and user information currently stored in the cloud in S3. Then the data is decomposed and later unified into logical storage units in a data warehouse. 

In its simplest form, a data pipeline is basically a recipe that allows the data expert to extract, cleanse, filter, enrich, aggregate, analyze, and transport the data. It is also important to mention here that this set of tasks must be performed in a specific and logical order and they might depend on other task. It would not make sense for the Sparkify team to try to analyze the data without extracting or screening it first. Airflow therefore uses the concept of DAG to illustrate the importance of organizing task, and creating dependencies and relationships between them. 


<a name="airflow"/>

## Airflow and Python

Airflow is an open source platform which was started in 2014 at Airbnb with the goal to author, schedule and monitor workflows. In Airflow, you define your DAGs using Python code that describe the structure of the corresponding pipeline.


|Characteristic|Explanation|
|---|----|
|Authoring:|Workflows in Airflow are written as directed acyclic graphs (DAGs) in Python.|
|Scheduling:|Users can determine when a workflow should start and end, after which interval it should run again|
|Monitoring:|Airflow provides an intuitive and interactive interface to monitor your workflow (See image below)|

<p align="center">
<img  width="100%" height= "100%" src=images/airflow_grid_ui.png alt="Grid View of Airflow">
</p>

Two advantages of using python:
1. Flexibility
2. No limits to creatitivity

The data engineer is not limited to a no-code or low-code user interface for creating pipelines. Since each task within the pipeline is being scripted, the data professional has full control of what the tasks should do. As previously pointed out, every DAG file is in essence a Python script. This type of flexibility provides developers with a high degree of tailoring when creating their data pipelines. Thus, it is possible to implement any task that can be implemented with Python and most of its modules.



<a name="python"/>

## How to run the Python DAG scripts

To run the provided Python-DAG script, the following criteria must be met:
- Airflow is installed locally, either as a normal installation or as a deployment in a container such as Docker.
- Airflow is running on AWS ECS. Other options are also possible.
- Access to an AWS account with permission to access S3 buckets and create Redshift clusters in the Oregon region.
- Create and save AWS and Redshift credentials in Airflow's connection settings.

Once the above requirements are met, you can run the sparkify.dag file by using Visual Studio Code.  

Although there is more than one Python file, such as a subdag file, four airflow operator files, and two sql-related files, only the sparkify_dag.py file is executed.

Due to current bugs in Airflow and special system configurations, there is no folder structure as intended. Most files are distributed in two folders only.


<a name="etl"/>

## The ETL Process

Below is not only a brief tabular summary of the 6 main tasks, but also a graphical representation of our Airflow pipeline. 


|Create tables Subdag|Staging tables|First Quality Check (# Rows)|Fact table|Dimension tables|Second Quality Check (# Rows)|
|---|---|---|---|---|---|
|staging_events|Stage_events|8056||||
|staging_songs|Stage_songs|14896||||
|songplays|||songplays_fact_table||6820|
|artists||||artist_dim|10025|
|songs||||song_dim|14896|
|users||||user_dim|104|
|time||||time_dim|6820|



<p align="center">
<img  width="100%" height= "100%" src=images/newfull_airflow_ui.png alt="Full Airflow Diagram">
</p>


<a name="files"/>

## Sparkify Data Files

The files used in this repository are all stored in two **S3 buckets**. These buckets contain the song dataset and the log dataset, both in json format.
```
Song data: s3://<s3_bucket>/<s3_key>

Log data: s3://<s3_bucket>/<s3_key>
```

**The song dataset**

The song dataset is a subset of the [MillionDataSet](http://millionsongdataset.com/). These files contain real metadata about songs and artists. As mentioned earlier, it is a subset that contains only the first three letters of the track ID of each song; hence the partitioning.

The song files have the following structure:
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Below we can see what attributes make up a song file:
```
{"artist_id":"ARJNIUY12298900C91","artist_latitude":null,"artist_location":"","artist_longitude":null,"artist_name":"Adelitas Way","duration":213.9424,"num_songs":1,"song_id":"SOBLFFE12AF72AA5BA","title":"Scream","year":2009}
```



**The log Dataset**

Unlike the song files, which are real data, the log files are simulated files. They were created using an external tool ([event simulator](https://github.com/Interana/eventsim)). These files are divided by yeary and month; however, only generated data for the month of November of 2018 was used.

The log files have the following structure:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Below you can view an image of the contents of a log file.

<p align="center">
<img  width="100%" height= "100%" src=images/event_data.png alt="Log File">
</p>



<a name="result"/>

## Resulting ER Diagram

The resulting database organizational structure is a star schema, which is a wide-spread structure for data warehouse systems. Star schemes are optimized for analytical processing.

<p align="center">
<img  width="80%" height= "80%" src=images/sparkify_er.png alt="Sparkify ER Diagram">
</p>