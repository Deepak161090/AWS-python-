import boto3
# Get the S3 client
aws_access_key_id = ''
aws_secret_access_key = ''
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
bucket_name ='' 
source_folder = 'input'
destination_folder = 'output'	
# Get the source and destination folders
def move_files(bucket_name, source_folder, destination_folder):
    files_to_delete = []
    # Get the list of objects in the source folder
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
    # Loop through the objects and move them to the destination folder
    try:
        for obj in objects['Contents']:
            key = obj['Key']  # Extract the key (filename) of the object
            new_key = key.replace(source_folder, destination_folder, 1) # Construct the new key for the destination folder
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': key},
                Key=new_key
            )
            files_to_delete.append({"Key": obj["Key"]})
            
        response = s3.delete_objects(
            Bucket=bucket_name, Delete={"Objects": files_to_delete}
        )
        print("The Files have been moved successfully ")
    except:
        print("There are no Files in the Source")
        
        
move_files(bucket_name, source_folder, destination_folder)