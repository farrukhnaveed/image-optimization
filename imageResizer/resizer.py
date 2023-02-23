import PIL
from PIL import Image
import os 
from dotenv import load_dotenv
load_dotenv()

PATH = "/app/imageOutput/enhanced/"
DEST_OPTIMIZED = "/app/imageOutput/optimized/"
DEST_REDUCED = "/app/imageOutput/reduced/"
IMAGE_RESOLUTION_THRESHOLD_WIDTH = int(os.getenv('IMAGE_RESOLUTION_THRESHOLD_WIDTH'))
IMAGE_RESOLUTION_THRESHOLD_HEIGHT = int(os.getenv('IMAGE_RESOLUTION_THRESHOLD_HEIGHT'))  
IMAGE_QUALITY_THRESHOLD = int(os.getenv('IMAGE_QUALITY_THRESHOLD'))  

def optimize(reduce = False, quality = None):
    if reduce == True:
        input_path = DEST_OPTIMIZED
        width_threshold = int(IMAGE_RESOLUTION_THRESHOLD_WIDTH / 2)
        height_threshold = int(IMAGE_RESOLUTION_THRESHOLD_HEIGHT / 2)
        output_path = DEST_REDUCED
        delete_source_image = False
    else:
        input_path = PATH
        width_threshold = IMAGE_RESOLUTION_THRESHOLD_WIDTH
        height_threshold = IMAGE_RESOLUTION_THRESHOLD_HEIGHT
        output_path = DEST_OPTIMIZED
        delete_source_image = True
    quality = IMAGE_QUALITY_THRESHOLD if quality is None else quality

    for image in os.listdir(input_path):
        percentage_change = 1
        image_name = image.split('.', 1)[0]
        imageObj = Image.open(input_path+image)
        width = imageObj.size[0]
        height = imageObj.size[1]
        if (imageObj.size[0] > width_threshold):
            width = width_threshold
            percentage_change = float(width/float(imageObj.size[0]))
            height = int( float(imageObj.size[1]) * percentage_change )
        elif (imageObj.size[1] > height_threshold):
            height = height_threshold
            percentage_change = float(height/float(imageObj.size[1]))
            width = int( float(imageObj.size[0]) * percentage_change )
        qualityPercentage = 100 if percentage_change > 1 else int(percentage_change * 100)
        quality = quality if qualityPercentage < quality else qualityPercentage

        imageObj = imageObj.resize((width, height), PIL.Image.NEAREST)
        imageObj.save(output_path+image_name+ '.webp', 'webp', optimize=True, quality=quality)
        if delete_source_image == True:
            os.unlink(input_path+image)
        file_stats = os.stat(output_path+image_name+ '.webp')
        return {
            "quality": quality,
            "width": width,
            "height": height,
            "size": file_stats.st_size / 1024
        }


def getSize():
    for image in os.listdir(DEST_OPTIMIZED):
        imageObj = Image.open(DEST_OPTIMIZED+image)
        width = imageObj.size[0]
        height = imageObj.size[1]
        file_stats = os.stat(DEST_OPTIMIZED+image)
        return {
            "width": width,
            "height": height,
            "size": file_stats.st_size / 1024
        }

if __name__ == "__main__":
    optimize()