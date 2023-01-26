
import cv2
import numpy as np
from Helpers import *
import os

path = "/app/images"
  
# Change the directory
os.chdir(path)

def test():
    images = []
    for file in os.listdir():

        file_path = f"{path}/{file}"
        # f = open(file_path, "r")
        # filestr = cv2.imread(file_path)
        with open(file_path, 'rb') as f:
              filestr = f.read()

        npimg = np.frombuffer(filestr, np.uint8)
        image = cv2.imdecode(npimg, cv2.IMREAD_UNCHANGED)
        # image = Helpers.resize(image, height = 500)

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        fm = cv2.Laplacian(gray, cv2.CV_64F).var()
        result = "Not Blurry"

        if fm < 100:
            result = "Blurry"

        sharpness_value = "{:.0f}".format(fm)
        message = [file,result,sharpness_value]
        images.append(message);

    print(images)



if __name__ == "__main__":
    # print(__file__)
    test()
    # app.run(debug=True)