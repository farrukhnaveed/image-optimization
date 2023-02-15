import requests # request img from web
import shutil # save img locally
import mimetypes
import os

INPUT_RAW = "/app/imageInput/raw"
INPUT_ENHANCE = "/app/imageInput/enhance"
OUTPUT_ENHANCED = "/app/imageOutput/enhanced"
OUTPUT_OPTIMIZED = "/app/imageOutput/optimized"

def cleanFolder(path = None):
    if (path == None):
        path = [INPUT_RAW, INPUT_ENHANCE, OUTPUT_ENHANCED, OUTPUT_OPTIMIZED]
    else:
        path = [path]

    for DEST in path:
        os.makedirs(DEST, exist_ok=True)

    for DEST in path:
        for filename in os.listdir(DEST):
            file_path = os.path.join(DEST, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

def download(url, id, table):
    file_name = "{}@@@{}".format(id, table)
    res = requests.get(url, stream = True)
    extension = mimetypes.guess_extension(res.headers.get('content-type', '').split(';')[0]) 

    if res.status_code == 200:
        absolute_file_name = "{}/{}{}".format(INPUT_RAW, file_name, extension or '.jpg')
        with open(absolute_file_name,'wb') as f:
            shutil.copyfileobj(res.raw, f)
        print('Image sucessfully Downloaded: ',absolute_file_name)
    else:
        print('Image Couldn\'t be retrieved')