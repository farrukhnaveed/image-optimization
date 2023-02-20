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

def execute(query, type = 'select'):
    result = []
    try:
        cnx = mysql.connector.connect(**mysqlConfig)
        cursor = cnx.cursor()
        cursor.execute(query)
        if type == 'insert':
            cnx.commit()
            result = [cursor.lastrowid]
        else:
            result = cursor.fetchall()
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()
    return result

def get_last_queued_image_id(source):
    id = None
    query = "select max(source_id) as source_id from fc_photos_optimization_queue where source = '{}'".format(source)
    result = execute(query)
    
    for (source_id,) in result:
        if source_id == None:
            id = 0
        else:
            id = source_id
    return id


def add_queue(source, offset, limit):
    if source == "fc_rental_photos":
        image = "CONCAT('https://img.cuddlynest.com/images/listings/', product_image_dir, product_image)"
    else:
        image = ""
    
    query = "SELECT '{}', id, {}, 'added in queue from DAG' FROM {} LIMIT {} OFFSET {}".format(source, image, source, limit, offset)
    result = execute(query)

    for (table, id, imageUrl, message) in result:
        query = "INSERT INTO fc_photos_optimization_queue (source, source_id, image, message) VALUES ('{}', {}, '{}', '{}')".format(table, id, imageUrl, message)
        queue_id = execute(query, 'insert')
        log(queue_id[0], source, id, 'pending', message)

def log(queue_id, source, source_id, status, message, step = None, error = None, detail = None):
    insertColumns = []
    insertValues = []
    if step == None:
        step = status
    if error:
        insertColumns.append('error')
        insertValues.append(error)
    if detail:
        insertColumns.append('detail')
        insertValues.append(detail)

    if len(insertColumns) > 0:
        for i in range(0, len(insertColumns)):
            if i == 0:
                insertColumns[i] = "," + insertColumns[i]
            insertColumns[i] = "'" + insertColumns[i] + "'"
            insertValues[i] = "'" + insertValues[i] + "'"
            if i + 1 == len(insertColumns):
                insertValues[i] = "," + insertValues[i]
    query = "INSERT INTO fc_photos_optimization_queue_audit_log (queue_id, source, source_id, step, message, status {}) VALUES ({}, '{}', {}, '{}', '{}', '{}' {})".format(",".join(insertColumns), queue_id, source, source_id, step, message, status, ",".join(insertValues))
    execute(query, 'insert')


def getPendingQueueIds(limit):
    ids = []
    query = "select * from fc_photos_optimization_queue where status = 'pending' limit %s" % limit
    result = execute(query)
    for obj in result:
        ids.append(obj[0])

    return ids

def updateQueueStatus(ids):
    status = 'pushed'
    message = 'pushed to queue'
    query = "UPDATE fc_photos_optimization_queue set status = '{}', message = '{}' where id in ( {} )".format(status, message, ",".join([str(k) for k in ids]))
    execute(query, 'insert')

    query = "SELECT id, source, source_id from fc_photos_optimization_queue where id in ( {} )".format(",".join([str(k) for k in ids]))
    result = execute(query)
    for (id, source, source_id) in result:
        log(id, source, source_id, status, message)

