# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 10:36:04 2023

@author: ddabral
"""
import boto3
# Get the S3 client
s3 = boto3.client('s3')
ssm = boto3.client('ssm')
# Get the source and destination folders
S3_BUCKET = ssm.get_parameter(Name='/deepak/S3_BUCKET')['Parameter']['Value']
source_folder = 'clari_data'
destination_folder = 'archive'

# Get the source and destination folders
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
        
        
move_files(S3_BUCKET, source_folder, destination_folder)