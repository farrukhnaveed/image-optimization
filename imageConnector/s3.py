import os, random, string
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from imageConnector import db
load_dotenv()

ACCESS_KEY = os.getenv("S3_KEY")
SECRET_KEY = os.getenv("S3_SECRET")
BUCKET_NAME = os.getenv("S3_BUCKET")
S3_BASE_PATH = os.getenv("S3_BASE_PATH")
LOCAL_OPTIMIZED_PATH = '/app/imageOutput/optimized/'
LOCAL_REDUCED_PATH = '/app/imageOutput/reduced/'


def upload_to_aws(local_file, dir, file_name):
    filename, file_extension = os.path.splitext(local_file)
    current_datetime = datetime.now()
    response = {
        "local_file": local_file,
        # "dir": current_datetime.strftime("%Y/%m/%d/%H/"),
        "dir": dir,
        "file_name": file_name + file_extension,
        "file_type": file_extension.split(".")[1],
        "status": False,
        "message": ""
    }

    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    s3_file = "{}/{}{}".format(S3_BASE_PATH, response["dir"], response["file_name"])
    try:
        s3.upload_file(local_file, BUCKET_NAME, s3_file)
        response["status"] = True
        response["message"] = "Upload Successful"
    except FileNotFoundError:
        response["message"] = "The file was not found"
    except NoCredentialsError:
        response["message"] = "Credentials not available"
    return response


def upload(reduce_path = False):
    response = []
    path = LOCAL_REDUCED_PATH if reduce_path else LOCAL_OPTIMIZED_PATH
    for image in os.listdir(path):
        id, table = image.split('.', 1)[0].split("@@@")
        dir, title = db.get_file_name(id, table)
        
        filename, file_extension = os.path.splitext(title)
        filename = filename + '_half' if reduce_path else filename + '_full'
        result = upload_to_aws(path+image, dir, filename)
        result["id"] = id
        result["table"] = table
        response.append(result)
    return response

if __name__ == "__main__":
    upload()