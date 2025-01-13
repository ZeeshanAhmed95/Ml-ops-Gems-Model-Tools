from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import sys
import os
import numpy as np
from src.pipeline.training_pipeline import TrainingPipeline
from src.logger.logging import logging
from src.exception.exception import customexception
from src.utils.utils import upload_file_gdrive

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

    @task()
    def data_ingestion():
        """Ingesting data from sources"""
        try:
            train_data_path,test_data_path=training_pipeline.start_data_ingestion()
            ingestion_result = {"train_data_path": train_data_path, "test_data_path": test_data_path}
            return ingestion_result
        
        except Exception as e:
            print(e)
            # logging.info('Exception occured during data ingestion in airflow training:', e)
            # raise customexception(e,sys)
        
    @task()
    def data_transformation(ingestion_result):
        """Transform the extracted data."""
        try:
            train_arr,test_arr=training_pipeline.start_data_transformation(ingestion_result["train_data_path"],ingestion_result["test_data_path"])
            transformation_result = {"train_arr":train_arr,"test_arr":test_arr}
            return transformation_result
        except Exception as e:
            print(e)

    @task()
    def model_trainer(transformation_result):
        """Training the model."""
        try:
            train_arr=np.array(transformation_result["train_arr"])
            test_arr=np.array(transformation_result["test_arr"])
            model_path = training_pipeline.start_model_training(train_arr,test_arr)
            training_result = {"model_path": model_path}
            return training_result
        except Exception as e:
            print(e)
    @task()
    def push_to_s3_gdrive(training_result):
        """
        Airflow task to upload the model to Google Drive.
        
        Args:
            training_result (dict): A dictionary containing training results, including the model path.
        """
        # Extract model file path and name
        model_file_path = training_result["model_file_path"]  # Example key for model file path
        # model_file_name = training_result["model_file_name"]  # Example key for model file name

        # model_file_path = r'artifacts\model.pkl'
        model_file_name = 'model.pkl'    

        mime_type = "application/octet-stream"

        if not os.path.exists(model_file_path):
            raise FileNotFoundError(f"The file {model_file_path} does not exist.")


        upload_file_gdrive(
            file_path=model_file_path,
            file_name=model_file_name,
            mime_type=mime_type
        )

        return f"Model '{model_file_name}' uploaded successfully to Google Drive."
        # os.system(f"aws s3 sync {artifact_folder} s3:/{bucket_name}/artifact")
    
    ## DAG Worflow- ETL Pipeline
    ingestion_result = data_ingestion()
    transformed_data=data_transformation(ingestion_result)
    training_result = model_trainer(transformed_data)
    push_to_s3_gdrive(training_result)


        
    




    

