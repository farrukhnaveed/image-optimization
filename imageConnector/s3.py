import os, random, string
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = os.getenv("S3_KEY")
SECRET_KEY = os.getenv("S3_SECRET")
BUCKET_NAME = os.getenv("S3_BUCKET")
S3_BASE_PATH =  os.getenv("S3_S3_BASE_PATH")
LOCAL_BASE_PATH = '/app/imageOutput/optimized/'


def upload_to_aws(local_file):
    filename, file_extension = os.path.splitext(local_file)
    current_datetime = datetime.now()
    response = {
        "local_file": local_file,
        "dir": current_datetime.strftime("%Y/%m/%d/%H/"),
        "file_name": ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16)) + file_extension,
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


def upload():
    response = []
    for image in os.listdir(LOCAL_BASE_PATH):
        id, table = image.split('.', 1)[0].split("@@@")
        result = upload_to_aws(LOCAL_BASE_PATH+image)
        result["id"] = id
        result["table"] = table
        response.append(result)
    return response

def test():
    test_file = "/app/imageOutput/optimized/1@@@BKP_fc_rental_photos.webp"
    uploaded = upload_to_aws(test_file)
    print(uploaded)

if __name__ == "__main__":
    test()