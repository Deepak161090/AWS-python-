import json
import requests
import logging
import time
import datetime
import boto3
import io
import gzip

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    def move_files(S3_BUCKET, source_folder, destination_folder):
        files_to_delete = []
        # Get the list of objects in the source folder
        objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=source_folder)
        # Loop through the objects and move them to the destination folder
        try:
            for obj in objects['Contents']:
                key = obj['Key']  # Extract the key (filename) of the object
                new_key = key.replace(source_folder, destination_folder, 1) # Construct the new key for the destination folder
                s3.copy_object(
                    Bucket=S3_BUCKET,
                    CopySource={'Bucket': S3_BUCKET, 'Key': key},
                    Key=new_key
                )
                files_to_delete.append({"Key": obj["Key"]})
                
            response = s3.delete_objects(
                Bucket=S3_BUCKET, Delete={"Objects": files_to_delete[1:]}
            )
            print("The Files have been moved successfully ")
        except:
            print("There are no Files in the Source")        
            
    def upload_json_gz(s3client, bucket, key, obj, default=None, encoding='utf-8'):
        """ upload python dict into s3 bucket with gzip archive 
        s3client: s3 instance
        bucket: s3 bucket
        key: s3 location
        obj: json data
        """   
        inmem = io.BytesIO()
        with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
            with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
                wrapper.write(json.dumps(obj, ensure_ascii=False, default=default))
        inmem.seek(0)
        s3client.put_object(Bucket=bucket, Body=inmem, Key=key)
        
    def get_data(api_key, job_url, job_id):
        """
        :param
            api_key: creds to connect to API
            job_url: endpoint url to get the forecast data
        :return:
            The forecast data in json format
        """
        headers = {
            'apikey': api_key
        }
        try:
            logger.info("Retrieving data for API: {0}".format(job_url))
            response = requests.get(job_url, headers=headers)
            res = response.json()         
            time.sleep(10)  
        except Exception as api_err:
            logger.error("Unable to retrieve data for job_id {1}: {0}".format(str(api_err), job_id))
            res = {}
        return res
    def get_job_id(api_key, forecast_url):
        """
        :param
            api_key: creds to connect to API
            forecast_url: endpoint url to get the job id
        :return:
            The job id for which forecast data will be pulled
        """
        payload = json.dumps({
            "typesToExport": [
                "forecast"
            ],
            "scopeId": {"type": "FORECAST_ROLE", "userId": 336717, "userType": "MGR"},
            "currency": "USD",
            "schedule": "NONE",
            "includeHistorical": "false"
        })

        headers = {
            'apikey': api_key
        }
        try:
            logger.info("Retrieving data for API: {0}".format(forecast_url))
            response = requests.post(forecast_url, headers=headers, data=payload)
            res = response.json()
        except requests.exceptions.RequestException as data_err:
            logger.error("Unable to retrieve job_id from API: {0}".format(str(data_err)))
            res = {}
        time.sleep(10)
        job_id=''
        if len(res) > 0:
            job_id = res['jobId']
        return job_id
    


    
    # Create S3 instance
    try:
        s3 = boto3.client('s3')
        ssm = boto3.client('ssm')
        S3_BUCKET = ssm.get_parameter(Name='/deepak/S3_BUCKET')['Parameter']['Value']
        logger.info("AWS connected successfully")
    except Exception as e:
        logger.error("Unable to create AWS S3 client: {0}".format(str(e)))
        s3 = None
        ssm = None
        S3_BUCKET = None
        
        
    # fetching values from ssm parameters store
    forecast_url=ssm.get_parameter(Name='/deepak/forecast_url')['Parameter']['Value']
    api_key = ssm.get_parameter(Name='/deepak/api_key')['Parameter']['Value']   
    
    #parameters for moving files in s3
    source_folder = 'clari_data'
    destination_folder = 'archive'
    
    # Calculate Current System date
    current_DT = datetime.datetime.now()
    year_DT = current_DT.year
    month_DT = current_DT.month
    day_DT = current_DT.day
    hour_DT = current_DT.hour
    
    # Prefix Month, Day and Hour with 0 in case it's a single digit value
    month_DT_str = '{0:02}'.format(month_DT)
    day_DT_str = '{0:02}'.format(day_DT)
    hour_DT_str = '{0:02}'.format(hour_DT)
    
    # This is rounded upto the hour
    s3_file_suffix = "{0}{1}{2}{3}00".format(year_DT, month_DT_str, day_DT_str, hour_DT_str)
    key_prefix = "clari_data/"
    file_name = "forecast_data_" + s3_file_suffix + ".json.gz"
    #calling the function to move files from clari_data to archive folder
    move_files(S3_BUCKET, source_folder, destination_folder)
    # Call get_job_id function to get job id from Clari API
    job_id = get_job_id(api_key, forecast_url)
    if len(job_id)>0:
        job_url = "https://api.clari.com/v4/export/jobs/"+job_id+"/results"
        data = get_data(api_key, job_url, job_id)
    else:
        logger.error("Error: Failed to get the job_id from Clari")
        data={}
    if len(data)>0:
        try:
            upload_json_gz(s3, S3_BUCKET, key_prefix+file_name, data)
            logger.info("JSON data uploaded successfully to S3 bucket:{0} and key {1}".format(S3_BUCKET,
                                                                                      key_prefix + file_name))
        except Exception as e:
            logger.error("Unable to write JSON to bucket {0} and key {1} because {2}".format(S3_BUCKET,
                                                                                      key_prefix + file_name,
                                                                                      str(e)))
    return {'status code': 200}

            
            
  