
import cv2
import numpy as np
import shutil
from .Helpers import *
import os

PATH = "/app/imageInput/raw"
DEST = "/app/imageInput/enhance"
BLUR_THRESHOLD = 100  
# Change the directory
os.chdir(PATH)

def test():
    images = isBlur()
    print(images)

def isBlur():
    images = []
    for file in os.listdir():

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
        result = {
            "isBlur": fm < BLUR_THRESHOLD,
            "sharpness_value": sharpness_value,
            "filePath": file_path
        }
        images.append(result);

    return images

def findBlur():
    images = isBlur()

    for image in images:
        if (image['isBlur'] == True):
            shutil.copy(image['filePath'], DEST)

if __name__ == "__main__":
    # print(__file__)
    findBlur()
    # app.run(debug=True)