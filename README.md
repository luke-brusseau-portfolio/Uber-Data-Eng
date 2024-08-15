# Uber-Data-Eng

Goal: Execute an end-to-end ETL pipeline within the GCP ecosystem from data extraction to storage, while also gaining hands-on experience with Mage by learning and implementing it effectively in the workflow.

Intro

This project delves into the processing and analysis of Uber ride statistics by harnessing the full capabilities of the Google Cloud Platform. By integrating services such as Google Cloud Storage, and Compute Engine, along with the data orchestration power of Mage, I transformed raw ride data into actionable insights. From initial data extraction through advanced transformation and loading into a data warehouse, every step of this project demonstrates how cloud-based tools can optimize and streamline the end-to-end workflow of managing and analyzing large-scale datasets.

Skills

Cloud Storage Management: Securely storing and managing large datasets in Google Cloud Storage.

Data Pipeline Orchestration: Building and automating end-to-end data workflows using Mage on Google Cloud Compute Engine.

Data Extraction: Retrieving data from cloud storage through API integration for seamless processing.

Data Transformation: Cleaning, enriching, and transforming raw data using Python for meaningful analysis.

Data Warehousing: Loading and organizing processed data into Google Cloud BigQuery for advanced querying and analytics.

Tools

Google Cloud Storage: For secure and scalable storage of large datasets.

Google Cloud Compute Engine: For provisioning and managing virtual machines to run data processing tasks.

Mage: For orchestrating and automating data pipelines.

Python: For transforming and processing data within the Mage platform.

Pandas: For helping with the data transformation by creating data frames.

Google Cloud BigQuery: For efficient data warehousing

Step-by-Step Breakdown

Data Storage in Google Cloud Storage

The project commenced with the storage of Uber ride statistics data in Google Cloud Storage, a scalable and secure storage solution. This initial step ensured that the raw data was securely stored and easily accessible for subsequent processing tasks. Google Cloud Storage provided the necessary infrastructure to handle the large volume of data, making it the ideal starting point for the pipeline.

The raw CSV file containing Uber ride statistics can be found in the Github Repository under uber_data.csv

Virtual Machine Setup with Google Cloud Compute Engine

To process the data, I created a virtual machine using Google Cloud Compute Engine. This VM provided the computational power needed to run data processing tasks. The environment was configured to support Mage, a powerful data pipeline tool, allowing for seamless integration with other Google Cloud services. Setting up the VM was a critical step in ensuring that the processing environment was both scalable and efficient.

Data Extraction with Mage

Once the environment was set up, I utilized Mage to orchestrate the data extraction process. I made Python code within Mage to make an API call to Google Cloud Storage, retrieving the Uber ride statistics CSV file. This step was crucial as it formed the basis for all further data processing, ensuring that the data was ready for transformation.

This API calling file can be found in the Github Repository under extract_uber_data.py

Data Transformation with Python in Mage

With the data extracted, the next step was to transform it into a more usable format. This was accomplished by writing Python code within Mage, where I performed various data cleaning and transformation tasks. This included handling duplicate values, normalizing data fields, and creating data frames in Pandas. The transformation process was essential for converting raw data into a structured format suitable for analysis.

This transformation file can be found in the GitHub repository under transform_uber_data.py

Data Loading into Google Cloud BigQuery

The final stage involved loading the transformed data into Google Cloud BigQuery for analysis. Another Python code block was created in Mage to handle the data-loading process. This step ensured that the data was efficiently stored in a data warehouse, ready for analytics.

This Loading file can be found in the Github Repository under load_BQ.py

Summary

This project has been instrumental in advancing both my technical skills and professional growth. It represents my first successful end-to-end project using Google Cloud Platform, where I managed the entire workflow from data extraction to preparing the data for future analysis. The experience provided me with a deep understanding of the various GCP services and how to integrate them efficiently. Additionally, I gained valuable insights into orchestrating data workflows with Mage, further enhancing my ability to leverage cloud-based tools to achieve project objectives.
