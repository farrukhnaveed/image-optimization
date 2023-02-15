import sys
import os
from imageBlurDetection.blur import findBlur
from imageEnhancer.enhance import enhance
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from imageConnector import getImageUrl
from imageConnector.request import download, cleanFolder
from imageResizer.resizer import optimize
from imageConnector.s3 import upload
from imageConnector.db import updateData

def process(data):
    imagesData = getImageUrl(data)
    cleanFolder()

    for imageData in imagesData:
        download(imageData['url'], imageData['id'], imageData['table'])
        
    findBlur()
    enhance()
    optimize()
    response = upload()
    for imageObj in response:
        if imageObj["status"] == True:
            updateData(data, imageObj)
    


if __name__ == "__main__":
    print(__file__)
    # findBlur()
    # enhance()
    # optimize()
    # app.run(debug=True)