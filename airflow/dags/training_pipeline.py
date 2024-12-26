from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import sys
import os
import numpy as np
from src.pipeline.training_pipeline import TrainingPipeline
from src.logger.logging import logging
from src.exception.exception import customexception

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}
training_pipeline=TrainingPipeline()

## DAG
with DAG(dag_id='gem_training_pipeline',
         default_args=default_args,
         description="it is my training pipeline",
         schedule_interval='@daily',
         tags=["machine_learning ","classification","gemstone"],
         catchup=False) as dag:
    
    dag.doc_md = __doc__

    @task()
    def data_ingestion():
        """Ingesting data from sources"""
        try:
            train_data_path,test_data_path=training_pipeline.start_data_ingestion()
            return train_data_path,test_data_path
        
        except Exception as e:
            logging.info('Exception occured during data ingestion in airflow training:', e)
            raise customexception(e,sys)
        
    @task()
    def data_transformation(train_data_path,test_data_path):
        """Transform the extracted weather data."""
        train_arr,test_arr=training_pipeline.start_data_transformation(train_data_path,test_data_path)
        return train_arr,test_arr
    
    @task()
    def model_trainer(train_arr,test_arr):
        """Load transformed data into PostgreSQL."""
        train_arr=np.array(train_arr)
        test_arr=np.array(test_arr)
        training_pipeline.start_model_training(train_arr,test_arr)

    @task()
    def push_to_s3():
        bucket_name=os.getenv("BUCKET_NAME")
        artifact_folder="/app/artifacts"
        os.system(f"aws s3 sync {artifact_folder} s3:/{bucket_name}/artifact")
    
    ## DAG Worflow- ETL Pipeline
    train_data_path,test_data_path= data_ingestion()
    transformed_data=data_transformation(train_data_path,test_data_path)
    model_trainer(transformed_data)
    push_to_s3()


        
    




    

