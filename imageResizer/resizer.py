import PIL
from PIL import Image
import os 

PATH = "/app/imageOutput/enhanced/"
DEST = "/app/imageOutput/optimized/"
MIN_DIMENSION = 200

def optimize():
    for image in os.listdir(PATH):
        image_name = image.split('.', 1)[0]
        fixed_width = 1000
        imageObj = Image.open(PATH+image)
        width_percent = (fixed_width / float(imageObj.size[0]))
        height_size = int( float(imageObj.size[1]) * float(width_percent) )
        imageObj = imageObj.resize((fixed_width, height_size), PIL.Image.NEAREST)
        imageObj.save(DEST+image_name+ '.webp', 'webp', optimize=True, quality=90)
        os.unlink(PATH+image)

if __name__ == "__main__":
    optimize()