import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode

load_dotenv()
mysqlConfig = {
    "user": os.getenv("MYSQL_USERNAME"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "host": os.getenv("MYSQL_HOST"),
    "database": os.getenv("MYSQL_DATABASE"),
    "raise_on_warnings": True,
}


def getData(data):
    imagesData = []
    try:
        cnx = mysql.connector.connect(**mysqlConfig)
        cursor = cnx.cursor()

        query = "SELECT {},id FROM {} WHERE id in ( {} )".format(
            data["columns"], data["table"], data["imageIds"]
        )
        cursor.execute(query)
        columns = data["columns"].split(",")

        urlSplit = data["urlPattern"].split("{}")

        for columns in cursor:
            index = 0
            url = ""
            for urlPart in urlSplit:
                index = index + 1
                if index < len(urlSplit):
                    url = url + urlPart + columns[index - 1]

            image = {"id": columns[2], "table": data["table"], "url": url, "meta": {}}
            index = 0
            for key in data["columns"].split(","):
                image["meta"][key] = columns[index]
                index = index + 1
            imagesData.append(image)

        cursor.close()
        cnx.close()

        return imagesData
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()


def updateData(data, imageObj):
    imagesData = []
    try:
        cnx = mysql.connector.connect(**mysqlConfig)
        cursor = cnx.cursor()

        query = "UPDATE {} SET product_image='{}', product_image_dir='{}', original_image_extension='{}' WHERE id = {}".format(
            imageObj["table"], imageObj["file_name"], imageObj["dir"], imageObj["file_type"], imageObj["id"]
        )
        cursor.execute(query)
        cnx.commit()

        cursor.close()
        cnx.close()

        return imagesData
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()

