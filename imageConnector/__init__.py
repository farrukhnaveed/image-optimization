from db import getData

def getImageUrl(data):
    if data["source"] == "db":
        imagesData = getData(data)
    else:
        imagesData = []

    return imagesData