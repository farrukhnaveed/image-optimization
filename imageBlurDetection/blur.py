
import cv2
import PIL
from PIL import Image
import os
import numpy as np
import shutil
from .Helpers import *
import os
from dotenv import load_dotenv
load_dotenv()

PATH = "/app/imageInput/raw"
DEST = "/app/imageInput/enhance"
OPTIMIZE_DEST = "/app/imageOutput/optimise"
IMAGE_BLUR_THRESHOLD = int(os.getenv('IMAGE_BLUR_THRESHOLD'))
IMAGE_RESOLUTION_THRESHOLD_WIDTH = int(os.getenv('IMAGE_RESOLUTION_THRESHOLD_WIDTH'))
IMAGE_RESOLUTION_THRESHOLD_HEIGHT = int(os.getenv('IMAGE_RESOLUTION_THRESHOLD_HEIGHT')) 
# Change the directory
# os.chdir(PATH)

def test():
    images = isBlur()
    print(images)

def isBlur():
    images = []
    for file in os.listdir(PATH):

        file_path = f"{PATH}/{file}"
        # f = open(file_path, "r")
        # filestr = cv2.imread(file_path)
        with open(file_path, 'rb') as f:
              filestr = f.read()

        npimg = np.frombuffer(filestr, np.uint8)
        image = cv2.imdecode(npimg, cv2.IMREAD_UNCHANGED)
        # image = Helpers.resize(image, height = 500)

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        fm = cv2.Laplacian(gray, cv2.CV_64F).var()

        sharpness_value = "{:.0f}".format(fm)
        imageObj = Image.open(file_path)
        result = {
            "isBlur": fm < IMAGE_BLUR_THRESHOLD,
            "shouldEnhance": imageObj.size[0] < IMAGE_RESOLUTION_THRESHOLD_WIDTH or imageObj.size[1] < IMAGE_RESOLUTION_THRESHOLD_HEIGHT,
            "sharpness_value": sharpness_value,
            "filePath": file_path
        }
        images.append(result)

    return images

def findBlur():
    images = isBlur()

    for image in images:
        if (image['isBlur'] == True or image['shouldEnhance'] == True):
            shutil.copy(image['filePath'], DEST)
        else:
            shutil.copy(image['filePath'], OPTIMIZE_DEST)
        os.unlink(image['filePath'])
    return images

def transferEnhance():
    for file in os.listdir(PATH):
        file_path = f"{PATH}/{file}"
        shutil.copy(file_path, DEST)
        os.unlink(file_path)

def transferOptimize():
    for file in os.listdir(PATH):
        file_path = f"{PATH}/{file}"
        shutil.copy(file_path, OPTIMIZE_DEST)
        os.unlink(file_path)


if __name__ == "__main__":
    # print(__file__)
    findBlur()
    # app.run(debug=True)