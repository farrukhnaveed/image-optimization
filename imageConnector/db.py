import os, json
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
    image = ""
    destination = ""
    message = 'added in queue from DAG'
    result = []
    if source == "fc_rental_photos":
        destination = "fc_rental_photos"
        # TODO: make bucket name dynamic
        image = "CONCAT('https://img.cuddlynest.com/images/listings/', product_image_dir, product_image)"
        
        query = "SELECT id, {} FROM {} LIMIT {} OFFSET {}".format(image, source, limit, offset)
        result = execute(query)

    for (id, imageUrl) in result:
        allow_blur = 1
        allow_optimize = 1
        allow_reduce = 1
        allow_resolution_size = 1
        query = "INSERT INTO fc_photos_optimization_queue (source, source_id, destination, image, message, allow_blur, allow_optimize, allow_reduce, allow_resolution_size) VALUES ('{}', {}, '{}', '{}', '{}', {}, {}, {}, {})".format(source, id, destination, imageUrl, message, allow_blur, allow_optimize, allow_reduce, allow_resolution_size)
        queue_id = execute(query, 'insert')
        log(queue_id[0], message)

def log(queue_id, message, step = None, error = None, detail = None):
    # , destination, allow_blur, allow_optimize, allow_reduce, allow_resolution_size
    query = "SELECT status, source, source_id, is_blur, should_enhance, is_enhanced, is_optimized, is_reduced FROM fc_photos_optimization_queue WHERE id = %s" % queue_id
    result = execute(query)
    
    for (status, source, source_id, is_blur, should_enhance, is_enhanced, is_optimized, is_reduced) in result:
        queue_state = json.dumps({
            "status":status,
            "is_blur":is_blur,
            "should_enhance":should_enhance,
            "is_enhanced":is_enhanced,
            "is_optimized":is_optimized,
            "is_reduced":is_reduced
        })
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
        query = "INSERT INTO fc_photos_optimization_queue_audit_log (queue_id, status, source, source_id, step, message, queue_state {}) VALUES ({}, '{}', '{}', {}, '{}', '{}', '{}' {})".format(",".join(insertColumns), queue_id, status, source, source_id, step, message, queue_state, ",".join(insertValues))
        execute(query, 'insert')


def getPendingQueueIds(limit):
    ids = []
    query = "select * from fc_photos_optimization_queue where status = 'pending' limit %s" % limit
    result = execute(query)
    for obj in result:
        ids.append(obj[0])

    return ids

def updateQueue(id, data):
    keys = list(data.keys())
    values = list(data.values())

    for idx, x in enumerate(values):
        if ( x.isnumeric() ):
            values[idx] = keys[idx] + "=" + str(x)
        else:
            values[idx] = keys[idx] + "='" + str(x) + "'"
    query = "UPDATE fc_photos_optimization_queue set {} where id = {}".format(",".join(values), str(id))
    execute(query, 'insert')

