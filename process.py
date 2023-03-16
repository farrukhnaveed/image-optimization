import sys
import os
import json
from imageBlurDetection.blur import isBlur, transferEnhance,  transferEnhanced, transferOptimize
from imageEnhancer.enhance import enhance
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from imageConnector import getImageUrl
from imageConnector.request import download, cleanFolder
from imageResizer.resizer import optimize, getSize
from imageConnector.s3 import upload
from imageConnector import db

def process(queue_obj):
    cleanFolder()
    
    #download step
    db.log(queue_obj['id'], 'Starting image download', 'download')
    result = download(queue_obj['image'], queue_obj['source_id'], queue_obj['source'])
    if (result["status"] == False):
        db.updateQueue(queue_obj['id'] ,{
            "status": "rejected",
            "message": result["message"]})
        db.log(queue_obj['id'], 'unable to download image', 'download', '404', result["message"])
        return

    if queue_obj["allow_blur"] == True:
        db.log(queue_obj['id'], 'Checking blur', 'blur')
        blurImages = isBlur()
        if (blurImages[0]['shouldEnhance'] == False):
            db.updateQueue(queue_obj['id'] ,{
                "is_blur": 1 if blurImages[0]['isBlur'] == True else 0,
                "message": "image does not need enhancement"
            })
            db.log(queue_obj['id'], "image does not need enhancement", 'blur')
            queue_obj["allow_optimize"] = False
            # transferOptimize()
            transferEnhanced()
        else:
            transferEnhance()
            db.updateQueue(queue_obj['id'] ,{
                "message": "image needs enhancement",
                "is_blur": 1 if blurImages[0]['isBlur'] == True else 0,
                "should_enhance": 1 if blurImages[0]['shouldEnhance'] == True else 0
            })
            db.log(queue_obj['id'], "image needs enhancement", 'blur')  
    elif queue_obj["allow_optimize"] == True:
        transferEnhance()
    else:
        # transferOptimize()
        transferEnhanced()

    optimizeResult = {
        "quality": None
    }

    if queue_obj["allow_optimize"] == True:
        db.log(queue_obj['id'], "Starting enhancement", 'enhance')
        result = enhance(queue_obj["id"])

        if result == True:
            db.updateQueue(queue_obj['id'] ,{
                "message": "image enhanced",
                "is_enhanced": 1
            })

    db.log(queue_obj['id'], "Starting optimization", 'optimize')
    optimizeResult = optimize()
    db.log(queue_obj['id'], "image optimized to {}%".format(str(optimizeResult["quality"])), 'optimize')

    db.log(queue_obj['id'], "Starting optimized image upload", 'upload')
    response = upload()
    imageObj = response[0]
    if imageObj["status"] == True:
        data = {
            "width": optimizeResult["width"],
            "height": optimizeResult["height"],
            "size": optimizeResult["size"],
            "dir": imageObj["dir"],
            "file_name": imageObj["file_name"],
            "file_type": imageObj["file_type"],
            "resolution_type": 'large'
        }
        db.updateQueue(queue_obj['id'] ,{
            "message": "image optimized",
            "is_optimized": 1
        })
        db.log(queue_obj['id'], "optimized image uploaded", 'upload', None, json.dumps(data))

        db.log(queue_obj['id'], "Saving optimized image in DB", 'db')
        db.updateRecord(queue_obj, data)
        db.log(queue_obj['id'], "Saved optimized image in DB", 'db')
    else:
        db.log(queue_obj['id'], 'unable to upload optimized image', 'upload', '400', imageObj["message"])

    
    if queue_obj["allow_reduce"] == True:
        db.log(queue_obj['id'], "Starting reduction", 'reduce')
        reduceResult = optimize(True, optimizeResult["quality"])
        db.log(queue_obj['id'], "image reduced to 50%", 'reduce')

        db.log(queue_obj['id'], "Starting reduced image upload", 'upload')
        response = upload(True)
        imageObj = response[0]
        if imageObj["status"] == True:
            data = {
                "width": reduceResult["width"],
                "height": reduceResult["height"],
                "size": reduceResult["size"],
                "dir": imageObj["dir"],
                "file_name": imageObj["file_name"],
                "file_type": imageObj["file_type"],
                "resolution_type": 'medium'
            }
            db.log(queue_obj['id'], "reduced image uploaded", 'upload', None, json.dumps(data))
            db.log(queue_obj['id'], "Saving reduced image in DB", 'db')
            db.updateRecord(queue_obj, data)
            db.updateQueue(queue_obj['id'] ,{
                "message": "image reduced",
                "is_reduced": 1
            })
            db.log(queue_obj['id'], "Saved reduced image in DB", 'db')
        else:
            db.log(queue_obj['id'], 'unable to upload reduced image', 'upload', '400', imageObj["message"])

    if queue_obj["allow_optimize"] == False and queue_obj["allow_reduce"] == False and queue_obj["allow_resolution_size"] == True:
        db.log(queue_obj['id'], "Getting image resolution and size", 'size')
        result = getSize()
        db.log(queue_obj['id'], "Saving image resolution and size in db", 'db')
        db.updateSize(queue_obj, result)
        
    db.updateQueue(queue_obj['id'], {
        "status": "completed",
        "message": 'completed steps' 
    })


if __name__ == "__main__":
    print(__file__)
    # findBlur()
    # enhance()
    # optimize()
    # app.run(debug=True)