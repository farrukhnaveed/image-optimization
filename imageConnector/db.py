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
    # "init_command": "SET GLOBAL max_connections = 100000"
}
BUCKET_NAME = os.getenv("S3_BUCKET")
# BUCKET_NAME = 'img.cuddlynest.com' # for importing images

def updateRecord(queueObj, imageObj):
    if queueObj["source"] == "fc_rental_photos":
        query = "INSERT INTO {} (product_image_dir, product_image, original_image_extension, queue_id, resolution_width, resolution_height, image_size, resolution_type, s3_bucket, product_id, imgPriority, caption, status, imgtitle, mproduct_image) ( SELECT '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', product_id, imgPriority, caption, status, imgtitle, mproduct_image FROM {} where id = {} )".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, queueObj["source"], queueObj["source_id"])
    elif queueObj["source"] == "fc_rental_photos_optimized":
        query = "UPDATE {} SET product_image_dir = '{}', product_image = '{}', original_image_extension = '{}', queue_id = {},  resolution_width = {}, resolution_height = {}, image_size = {}, resolution_type ='{}', s3_bucket='{}' where id = {}".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, queueObj["source_id"] )
    else:
        query = "INSERT INTO {} (product_image_dir, product_image, original_image_extension, queue_id, resolution_width, resolution_height, image_size, resolution_type, s3_bucket, product_id) ( SELECT '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', product_id FROM {} where id = {} )".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, queueObj["source"], queueObj["source_id"])

    if query == "":
        log(queueObj["id"], "invalid source or query", "db", "query", queueObj['source'])
    else:
        execute(query, 'insert', queueObj["id"])


def updateSize(queueObj, imageObj):
    if queueObj["source"] == "fc_rental_photos_optimized":
        query = "UPDATE {} SET queue_id = {},  resolution_width = {}, resolution_height = {}, image_size = {} where id = {}".format(queueObj["destination"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], queueObj["source_id"] )
    elif queueObj["source"] == "fc_rental_photos":
        query = "INSERT INTO {} (product_image_dir, product_image, original_image_extension, queue_id, resolution_width, resolution_height, image_size, s3_bucket, product_id, imgPriority, caption, status, imgtitle, mproduct_image) ( SELECT product_image_dir, product_image, original_image_extension, {}, {}, {}, {}, '{}', product_id, imgPriority, caption, status, imgtitle, mproduct_image FROM {} where id = {} )".format(queueObj["destination"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], BUCKET_NAME, queueObj["source"], queueObj["source_id"])
    else:
        query = ""

    if query == "":
        log(queueObj["id"], "invalid source or query", "db", "query", queueObj['source'])
    else:
        execute(query, 'insert', queueObj["id"])



def execute(query, type = 'select', queue_id = None):
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
        if (queue_id is None):
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                error = str(err)
                detail = {
                    "query": query,
                    "error": error[:500]
                }
                print(detail)
        else:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                log(queue_id, "Invalid db credentials", 'db', 'query', query)
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                log(queue_id, "Database does not exist", 'db', 'query', query)
            else:
                error = str(err)
                detail = {
                    "query": query,
                    "error": error[:500]
                }
                log(queue_id, "Something went wrong", 'db', 'query', detail)
    else:
        cnx.close()
    return result

def get_last_queued_image_id(source):
    id = None
    query = "select max(source_id) as source_id from fc_photos_optimization_queue where source = '{}'".format(source)
    result = execute(query)
    
    for (source_id,) in result:
        if source_id is None:
            id = 0
        else:
            id = source_id
    return id


def add_queue(source, offset, limit):
    image = ""
    destination = ""
    message = 'added in queue from DAG'
    result = []
    if source == "fc_rental_photos" or source == "fc_rental_photos_optimized":
        destination = "fc_rental_photos_optimized"
        image = "CONCAT('https://%s/images/listings/', product_image_dir, product_image)" % BUCKET_NAME
        
        query = "SELECT id, {} FROM {} LIMIT {} OFFSET {}".format(image, source, limit, offset)
        result = execute(query)

    for (id, imageUrl) in result:
        allow_blur = 0
        allow_optimize = 0
        allow_reduce = 0
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
        if step is None:
            step = status
        if error is not None:
            insertColumns.append(',error')
            insertValues.append(",1")
        if detail is not None:
            insertColumns.append(',detail')
            detail = ",'%s'" % detail
            insertValues.append(detail[:500])

        query = "INSERT INTO fc_photos_optimization_queue_audit_log (queue_id, status, source, source_id, step, message, queue_state {}) VALUES ({}, '{}', '{}', {}, '{}', '{}', '{}' {})".format("".join(insertColumns), queue_id, status, source, source_id, step, message, queue_state, "".join(insertValues))
        execute(query, 'insert', queue_id)


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
        if ( isinstance(x, int) or (isinstance(x, str) and x.isnumeric())):
            values[idx] = keys[idx] + "=" + str(x)
        else:
            values[idx] = keys[idx] + "='" + str(x) + "'"
    query = "UPDATE fc_photos_optimization_queue set {} where id = {}".format(",".join(values), str(id))
    execute(query, 'insert')


def get_queue(queue_id):
    queue_obj = {
        "id": None,
        "source": '',
        "source_id": None,
        "destination": '',
        "image": '',
        'allow_blur': False,
        'allow_optimize': False,
        'allow_reduce': False,
        'allow_resolution_size': False
    }
    query = "select source, source_id, destination, image, allow_blur, allow_optimize, allow_reduce, allow_resolution_size from fc_photos_optimization_queue where id = {}".format(queue_id)
    result = execute(query)
    
    for (source, source_id, destination, image, allow_blur, allow_optimize, allow_reduce, allow_resolution_size) in result:
        queue_obj["id"] = queue_id
        queue_obj["source"] = source
        queue_obj["source_id"] = source_id
        queue_obj["destination"] = destination
        queue_obj["image"] = image
        queue_obj["allow_blur"] = True if allow_blur == 1 else False
        queue_obj["allow_optimize"] = True if allow_optimize == 1 else False
        queue_obj["allow_reduce"] = True if allow_reduce == 1 else False
        queue_obj["allow_resolution_size"] = True if allow_resolution_size == 1 else False
    return queue_obj



