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
    query = ""
    if queueObj["source"] == "fc_rental_photos":
        query = "INSERT INTO {} (product_image_dir, product_image, original_image_extension, queue_id, resolution_width, resolution_height, image_size, resolution_type, s3_bucket, product_id, imgPriority, caption, status, imgtitle, mproduct_image) ( SELECT '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', product_id, imgPriority, caption, status, imgtitle, mproduct_image FROM {} where id = {} )".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, queueObj["source"], queueObj["source_id"])
    elif queueObj["source"] == "fc_rental_photos_optimized":
        query = "UPDATE {} SET product_image_dir = '{}', product_image = '{}', original_image_extension = '{}', queue_id = {},  resolution_width = {}, resolution_height = {}, image_size = {}, resolution_type ='{}', s3_bucket='{}' where id = {}".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, queueObj["source_id"] )
    elif queueObj["source"] == "didatravel_properties_images":
        caption_query = "SELECT caption from {} where id = {}".format(queueObj["source"], queueObj["source_id"])
        result = execute(caption_query)
        
        for (caption,) in result:
            cn_caption = get_dida_caption(queueObj["id"], caption)
            # NOTE: product_id is mandatory in source table
            query = "INSERT INTO {} (product_image_dir, product_image, original_image_extension, queue_id, resolution_width, resolution_height, image_size, resolution_type, s3_bucket, caption, product_id) ( SELECT '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', '{}', cn_product_id FROM {} where id = {} )".format(queueObj["destination"],imageObj["dir"], imageObj["file_name"], imageObj["file_type"], queueObj["id"], imageObj["width"], imageObj["height"], imageObj["size"], imageObj["resolution_type"], BUCKET_NAME, cn_caption, queueObj["source"], queueObj["source_id"])

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


def add_queue(source, offset, limit, allow_blur = 0, allow_optimize = 0, allow_reduce = 0, allow_resolution_size = 1):
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
        save_queue(source, id, destination, imageUrl, message, allow_blur, allow_optimize, allow_reduce, allow_resolution_size)

def save_queue(source, source_id, destination, imageUrl, message, allow_blur = 0, allow_optimize = 0, allow_reduce = 0, allow_resolution_size = 1):
    query = "INSERT INTO fc_photos_optimization_queue (source, source_id, destination, image, message, allow_blur, allow_optimize, allow_reduce, allow_resolution_size) VALUES ('{}', {}, '{}', '{}', '{}', {}, {}, {}, {})".format(source, source_id, destination, imageUrl, message, allow_blur, allow_optimize, allow_reduce, allow_resolution_size)
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


def add_didatravel_properties_images_queue():
    query = """
        SELECT 
            id, 
            image as imageUrl,
            i.cn_product_id
        FROM 
        `didatravel_properties_images` i 
        inner join (
            SELECT 
            distinct cn_product_id 
            FROM 
            `didatravel_properties_images` 
            WHERE 
            cn_product_id != 0 
            and status_process = 'pending' 
            limit 
            1
        ) di on i.cn_product_id = di.cn_product_id 
        WHERE 
        i.status_process = 'pending'
    """
    result = execute(query)
    count = 0
    product_id = None
    for (id, imageUrl, cn_product_id) in result:
        count = count + 1
        product_id = cn_product_id
        save_queue('didatravel_properties_images', id, 'fc_rental_photos_optimized', imageUrl, 'added in queue from DAG', 1, 1, 1)

    if product_id is not None:
        query = " UPDATE didatravel_properties_images set status_process = 'ready_to_import' where cn_product_id = {} ".format(product_id)
        execute(query, 'insert')
    return count

def get_dida_caption(queue_id, caption):
    list = {
        "Business Center": "Others",
        "Featured Image": "Others",
        "Hotel Interior": "Others",
        "Hotel Lounge": "Others",
        "Interior Entrance": "Others",
        "Meeting Facility": "Others",
        "Guestroom View": "Others",
        "Interior Detail": "Others",
        "Property Amenity": "Others",
        "Spa": "Others",
        "Gift Shop": "Others",
        "In-Room Business Center": "Others",
        "Laundry Room": "Others",
        "Deleted": "Others",
        "Treatment Room": "Others",
        "Other": "Others",
        "Staircase": "Others",
        "Vending Machine": "Others",
        "Childrens Activities": "Others",
        "Concierge Desk": "Others",
        "Miscellaneous": "Others",
        "Ballroom": "Others",
        "Fireplace": "Others",
        "Hair Salon": "Others",
        "Spa Reception": "Others",
        "ATM or Banking On site": "Others",
        "Indoor Wedding": "Others",
        "Pet-Friendly": "Others",
        "Massage": "Others",
        "Spa Treatment": "Others",
        "Library": "Others",
        "Nail Salon": "Others",
        "Childrens Theme Room": "Others",
        "Childrens Play Area - Indoor": "Others",
        "Theater Show": "Others",
        "Arcade": "Others",
        "Gazebo": "Others",
        "Game Room": "Others",
        "Fountain": "Others",
        "Billiards": "Others",
        "Logo": "Others",
        "Archery": "Others",
        "Dock": "Others",
        "Pro Shop": "Others",
        "Day Care": "Others",
        "Point of Interest": "Others",
        "Others": "Others",
        "Chapel": "Others",
        "Karaoke Room": "Others",
        "Microwave": "Others",
        "Lounge": "Others",
        "Interior": "Others",
        "Television": "Others",
        "BBQ/Picnic Area": "Others",
        "Facial": "Others",
        "Check-in": "Others",
        "Room plan": "Others",
        "Casino": "Others",
        "休闲": "Others",
        "公共区域": "Others",
        "Golf Cart": "Others",
        "Activities": "Others",
        "Facilities": "Others",
        "Kids areas": "Others",
        "Check-in/Check-out Kiosk": "Others",
        "Equipment Storage": "Others",
        "Front of Property - Evening or Night": "Others",
        "Shopping mall": "Others",
        "ATM": "Others",
        "其他": "Others",
        "Recreational Facilities": "Others",
        "RV or Truck Parking": "Others",
        "Aqua Center": "Others",
        "Interior view": "Others",
        "Map": "Others",
        "View": "Others",
        "villa": "Others",
        "Shops": "Others",
        "Surrounding environment": "Others",
        "Children’s Activities": "Others",
        "Floor plans": "Others",
        "Meeting Room": "Others",
        "Beauty Salon": "Others",
        "Meeting room  or  ballrooms": "Others",
        "Indoor": "Others",
        "Safe": "Others",
        "Cleanliness badge": "Others",
        "Private Spa Tub": "Others",
        "Children’s Area": "Others",
        "Children’s Play Area - Indoor": "Others",
        "Children’s Theme Room": "Others",
        "Train Station Shuttle": "Others",
        "Superior": "Others",
        "Studio": "Others",
        "Golf course [on-site]": "Others",
        "Kid’s club": "Others",
        "Public areas": "Others",
        "Standar": "Others",
        "VIP 3": "Others",
        "Pub or Lounge": "Others",
        "Standard Air Conditioning": "Others",
        "Google Maps": "Others",
        "Club Class Room": "Others",
        "Laundry": "Others",
        "Land View from Property": "Others",
        "Beach Front": "Others",
        "Main Photo": "Others",
        "Happiness": "Others",
        "Standard A": "Others",
        "Deluxe Tent": "Others",
        "Standard-MAP(Including Breakfast Lunch or Dinner)": "Others",
        "Playground": "Others",
        "Giulio": "Others",
        "Front Elevation": "Others",
        "Business": "Others",
        "Cleanliness standards": "Others",
        "Apartment": "Others",
        "Yukata": "Others",
        "Japanese Style": "Others",
        "Rowing": "Others",
        "Heating": "Others",
        "Hotel Lilik": "Others",
        "General view": "Others",
        "Conferences": "Others",
        "Fashion Room": "Others",
        "Sports and Entertainment": "Others",
        "Single Premiere": "Others",
        "Hotel Facade": "Others",
        "Standard Double With Window": "Others",
        "Facade": "Others",
        "Standard No Window": "Others",
        "Executive Seviced Apartment": "Others",
        "Main": "Others",
        "Small Double Bed- Non-Smoking": "Others",
        "KALAVARA": "Others",
        "GRAND CENTRAL HOTEL": "Others",
        "Nearby Transport": "Others",
        "Standard Fan": "Others",
        "Superior Cottage": "Others",
        "Hotel Facility": "Others",
        "Twin": "Others",
        "Single Pod Side Entry": "Others",
        "Pantry Area": "Others",
        "Beach Front Air Condition Double": "Others",
        "Double Air Conditioning": "Others",
        "Fan": "Others",
        "Premier (North Tower)": "Others",
        "Japanese Quad": "Others",
        "V Lounge": "Others",
        "Suite with Breakfast": "Others",
        "Serviced Apartment": "Others",
        "Standard Villa": "Others",
        "Esha-Villa-Drupadi-2-Exterior-1": "Others",
        "Greta": "Others",
        "Aerial photo": "Others",
        "Studio Apartment": "Others",
        "Par": "Others",
        "main rane": "Others",
        "The Emperor Hotel Malacca": "Others",
        "Mesra Standard": "Others",
        "Living Hall": "Others",
        "Vacation Home": "Others",
        "Hotel Main Photo": "Others",
        "Superior 2D1N Fullboard (Lunch-Dinner-Breakfast)": "Others",
        "Premium": "Others",
        "Express Inn": "Others",
        "Superiorr": "Others",
        "Fruestueck": "Others",
        "Front door": "Others",
        "Superior Apartment": "Others",
        "Business Villa": "Others",
        "Ausenansicht": "Others",
        "Laguna Deluxe": "Others",
        "Beach Bungalow Half Board": "Others",
        "La Veranda": "Others",
        "Front view": "Others",
        "Hotel": "Others",
        "Aerial": "Others",
        "mainyard": "Others",
        "Executive": "Others",
        "Air Con bungalow": "Others",
        "Phra Ae Beach ( Long Beach )": "Others",
        "Chalet City View": "Others",
        "Bhakatapur Durbar Square": "Others",
        "glass": "Others",
        "Business Room": "Others",
        "NYC": "Others",
        "Business Twin": "Others",
        "Bungalow": "Others",
        "Cleanliness measures": "Others",
        "Four Squares": "Others",
        "Lockers": "Others",
        "Champagne service": "Others",
        "ATM/Banking On site": "Others",
        "Electric vehicle charging station": "Others",
        "Solarium": "Others",
        "Anchor House B&B": "Others",
        "Single - Domestic residents only": "Others",
        "Studio (3 Adults)": "Others",
        "Standard A - Domestic residents only": "Others",
        "Triple - Half Board": "Others",
        "inforecommendedhotels.net": "Others",
        "Business A": "Others",
        "Main Porch": "Others",
        "Super Deluxe": "Others",
        "2D1N Package (ex-Sukau)": "Others",
        "Elevator": "Others",
        "Guest wheelchair": "Others",
        "Luxury Family Accommodation": "Others",
        "Breakfast": "Others",
        "Deluxe Aircon Bungalow": "Others",
        "Tech": "Others",
        "Lanai": "Others",
        "Matrimonial Aircon without TV": "Others",
        "Emerald": "Others",
        "Boutique Suite In Building": "Others",
        "Smoking area": "Others",
        "Bungalow with Garden View": "Others",
        "Spa & Well-being": "Others",
        "Andaman Superior": "Others",
        "Umaya": "Others",
        "Designer store": "Others",
        "Home": "Others",
        "Japanese Style with Breakfast & Dinner (10 tatami mats)": "Others",
        "Japanese Style Standard Half Board - Honkan": "Others",
        "Comfy Studio": "Others",

        "Hotel Bar": "Bar",
        "Cafe": "Bar",
        "Nightclub": "Bar",
        "Poolside Bar": "Bar",
        "Bar": "Bar",
        "Bar or lounge": "Bar",
        "Beach Bar": "Bar",
        "Swim-up Bar": "Bar",
        "Varela Cafe": "Bar",
        "Veyo Bar": "Bar",

        "Bathroom Sink": "Bathroom",
        "Jetted Tub": "Bathroom",
        "Bathroom Amenities": "Bathroom",
        "Deep Soaking Bathtub": "Bathroom",
        "Shower": "Bathroom",
        "Accessible bathroom": "Bathroom",
        "Double shared bath": "Bathroom",
        "bathtub": "Bathroom",
        "Single Shared Bath": "Bathroom",
        "Japanese Style Shraed Bath and toilet": "Bathroom",

        "Living Room": "Bedroom",
        "In-Room Safe": "Bedroom",
        "In-Room Coffee": "Bedroom",
        "Mini-Refrigerator": "Bedroom",
        "Minibar": "Bedroom",
        "Courtyard View": "Bedroom",
        "Beach or Ocean View": "Bedroom",
        "Garden View": "Bedroom",
        "Mountain View": "Bedroom",
        "Room": "Bedroom",
        "View from Room": "Bedroom",
        "Bedroom": "Bedroom",
        "房间": "Bedroom",
        "Bed": "Bedroom",
        "Room Amenity": "Bedroom",
        "Double": "Bedroom",
        "Guest Room": "Bedroom",
        "Suite room": "Bedroom",
        "Double Room": "Bedroom",
        "Studio With Breakfast": "Bedroom",
        "Water view": "Bedroom",
        "Double or Twin Room": "Bedroom",
        "Economy Twin Room": "Bedroom",
        "Deluxe": "Bedroom",
        "Standard Room Only": "Bedroom",
        "Grazia Room (Standard)": "Bedroom",
        "Imperia Suite": "Bedroom",
        "Family Room with Window": "Bedroom",
        "Standard": "Bedroom",
        "Standard Double": "Bedroom",
        "Superior Double Room": "Bedroom",
        "Superior Double": "Bedroom",
        "Separate living room": "Bedroom",
        "Queen Bed": "Bedroom",
        "1 Bed Suite Smoking": "Bedroom",
        "Deluxe Single Room": "Bedroom",
        "View of interior": "Bedroom",
        "Deluxe Room": "Bedroom",
        "Executive Single Room": "Bedroom",
        "Extra Beds": "Bedroom",
        "Park View": "Bedroom",
        "Junior Suite": "Bedroom",
        "King Bed B": "Bedroom",
        "Premium Room": "Bedroom",
        "Standard Room": "Bedroom",
        "Executive Suite": "Bedroom",
        "Superior AC Room": "Bedroom",
        "Deluxe Non AC": "Bedroom",
        "Standard Air Cooled Room Only": "Bedroom",
        "Vintage Room": "Bedroom",
        "Deluxe Room With Breakfast": "Bedroom",
        "Premier Room": "Bedroom",
        "Superior King Room": "Bedroom",
        "Deluxe Double Bed": "Bedroom",
        "Royal Deluxe": "Bedroom",
        "Super Deluxe Room": "Bedroom",
        "Standard AC Room": "Bedroom",
        "City Mountain View Twin Room": "Bedroom",
        "King Bed": "Bedroom",
        "Suite": "Bedroom",
        "Single": "Bedroom",
        "Standard With Breakfast": "Bedroom",
        "King Bed B- Domestic residents only": "Bedroom",
        "Deluxe Triple": "Bedroom",
        "Single With Breakfast": "Bedroom",
        "Executive Queen Bed": "Bedroom",
        "King Room": "Bedroom",
        "Business Twin Room": "Bedroom",
        "Amenity (Guest room)": "Bedroom",
        "Standard Twin": "Bedroom",
        "Resort View": "Bedroom",
        "View of pool from Villa overlooking rice fields with mountains in distance": "Bedroom",
        "Premier Twin": "Bedroom",
        "Family Room (4 Adults)": "Bedroom",
        "Private 3 Bedroom": "Bedroom",
        "King Bed Room": "Bedroom",
        "2 Bedroom Service Suite": "Bedroom",
        "Standard Double Room": "Bedroom",
        "Superior King": "Bedroom",
        "Double bed": "Bedroom",
        "Couple Room": "Bedroom",
        "Queen Room": "Bedroom",
        "Superior Hollywood with Breakfast": "Bedroom",
        "Family Triple": "Bedroom",
        "3 Bedroom Twin Towers View": "Bedroom",
        "Superior Triple Room": "Bedroom",
        "Superior room with breakfast": "Bedroom",
        "Double Standard No Window": "Bedroom",
        "Standard Room Without Window": "Bedroom",
        "Deluxe Room (Ocean Wing)": "Bedroom",
        "Superior Garden View": "Bedroom",
        "Stay Superior Double with Balcony": "Bedroom",
        "Single Room": "Bedroom",
        "Single Room - Non-Smoking": "Bedroom",
        "Twin Room": "Bedroom",
        "Single Room - Smoking": "Bedroom",
        "Bunk Bed - Non-smoking": "Bedroom",
        "Single - Non-Smoking": "Bedroom",
        "Twin - Non-Smoking": "Bedroom",
        "Business King": "Bedroom",
        "Deluxe King": "Bedroom",
        "Standard Include one Breakfast": "Bedroom",
        "Deluxe Twin": "Bedroom",
        "Standard King": "Bedroom",
        "Deluxe Twin Bed": "Bedroom",
        "Standard A Twin with one breakfast": "Bedroom",
        "Night View": "Bedroom",
        "One-Bedroom Apartment": "Bedroom",
        "One Bed Room Apartment": "Bedroom",
        "1 Bedroom Apartment": "Bedroom",
        "Superior Room": "Bedroom",
        "2 Bedroom Home": "Bedroom",
        "Triple and Twin Room": "Bedroom",
        "Standard Air Conditioning Room": "Bedroom",
        "Business Double Bed": "Bedroom",
        "Superior Business Room": "Bedroom",
        "Business Suite": "Bedroom",
        "Deluxe Double Bed Room": "Bedroom",
        "Double Bed Room": "Bedroom",
        "Junior Suite 401": "Bedroom",
        "Economy Twin - Smoking": "Bedroom",
        "Single Room A": "Bedroom",
        "Single Room B": "Bedroom",
        "Double Room - Smoking": "Bedroom",
        "Small Double Bed -Non-Smoking": "Bedroom",
        "Deluxe Room with kitchen": "Bedroom",
        "Deluxe Double or Twin Bed": "Bedroom",
        "Premier City View - Best Available Rate": "Bedroom",
        "Superior Twin": "Bedroom",
        "8-Bed Dormitory - Female Only": "Bedroom",
        "Quad Room": "Bedroom",
        "Deluxe Double Room": "Bedroom",
        "Dormitory": "Bedroom",
        "Pulaoon Deluxe Room Only": "Bedroom",
        "Family room 3 person": "Bedroom",
        "Standard Garden View": "Bedroom",
        "Standard Sea View": "Bedroom",
        "Deluxe Studio": "Bedroom",
        "Suite Double or Twin (Main Building)": "Bedroom",
        "Suite Twin": "Bedroom",
        "Deluxe Studio 22H": "Bedroom",
        "Grand Deluxe Suite": "Bedroom",
        "Standard King Size Bedroom": "Bedroom",
        "2 Bedroom Executive Suite": "Bedroom",
        "3 Bedroom Grand Suite Type A": "Bedroom",
        "Superior Single": "Bedroom",
        "Two bedroom": "Bedroom",
        "Superior Queen Bed": "Bedroom",
        "Twin Bed": "Bedroom",
        "Mixed Dormitory ( 8 people)": "Bedroom",
        "Triple": "Bedroom",
        "Superior Double Bed": "Bedroom",
        "Riverside Room": "Bedroom",
        "Standard Single Room": "Bedroom",
        "Standard Double Bed": "Bedroom",
        "Standard Single": "Bedroom",
        "Deluxe Patial Sea View": "Bedroom",
        "Deluxe Suite with Bay View": "Bedroom",
        "Deluxe Business Room": "Bedroom",
        "Triple Room": "Bedroom",
        "Classic Double With Breakfast": "Bedroom",
        "Comfort Double": "Bedroom",
        "Twin Garden view": "Bedroom",
        "Deluxe Double or Twin Room": "Bedroom",
        "3 Bedroom Private Pool (Garden View)": "Bedroom",
        "1 Bedroom Pool Villa": "Bedroom",
        "Standard Double Bed (Room Only)": "Bedroom",
        "4 Bedroom Pool Villa": "Bedroom",
        "Twin Room En-suite": "Bedroom",
        "Executive Double Room": "Bedroom",
        "Executive Single": "Bedroom",
        "Deluxe Village Suite": "Bedroom",
        "Family Room (4 persons)": "Bedroom",
        "Standard with Window": "Bedroom",
        "Deluxe Double (Check In 6pm)": "Bedroom",
        "Deluxe Double": "Bedroom",
        "Single En-suite Room": "Bedroom",
        "Twin Suite": "Bedroom",
        "Double or Twin": "Bedroom",
        "Ciclamino Room": "Bedroom",
        "Business Double": "Bedroom",
        "1 Bedroom Apartment 2 Night": "Bedroom",
        "King": "Bedroom",
        "Standard Twin Bedded Room - Cirty View": "Bedroom",
        "Executive King Room": "Bedroom",
        "Superior King with Hill View": "Bedroom",
        "Superior Kingbedroom": "Bedroom",
        "Standard Twin Bed": "Bedroom",
        "Single - Smoking": "Bedroom",
        "Single Smoking": "Bedroom",
        "King Double Room": "Bedroom",
        "Executive Suite (4 persons)": "Bedroom",
        "Standard Single No Window": "Bedroom",
        "Superior Deluxe": "Bedroom",
        "3 Bedroom Apartment (6 Adults)": "Bedroom",
        "Chalet Sea View": "Bedroom",
        "3 Bedroom Vacation Stay": "Bedroom",
        "3 Bedroom Apartment": "Bedroom",
        "Superior Queen": "Bedroom",
        "3 Bedroom Home": "Bedroom",
        "Single Room with Private Bathroom": "Bedroom",
        "Loft Twin Beds": "Bedroom",
        "Double  or  Twin Standard Room": "Bedroom",
        "Deluxe Twin Room - Non-Smoking": "Bedroom",
        "Single Room A- Non-Smoking": "Bedroom",
        "Executive Room": "Bedroom",
        "Deluxe Family Room with Acropolis View": "Bedroom",
        "Standard Room With Breakfast": "Bedroom",
        "Superior Twin Bed": "Bedroom",
        "One Bedroom Apartment": "Bedroom",
        "Family Room for 3 People": "Bedroom",
        "Standard Double or Twin": "Bedroom",
        "Single with Shared Bathroom": "Bedroom",
        "Double Room with private bathroom": "Bedroom",
        "Room for 6 person": "Bedroom",
        "Double Room ( 2 people)": "Bedroom",
        "Quadruple Room with Shared Bathroom": "Bedroom",
        "Triple With Breakfast": "Bedroom",
        "Beach View": "Bedroom",
        "10-Bed Dormitory Shared Facilities": "Bedroom",
        "Standard Triple": "Bedroom",
        "1 Bed in 4-Bed Mixed Dormitory (Private bathroom)": "Bedroom",
        "Superior Sea View Twin Bed": "Bedroom",
        "Deluxe Ocean View Room": "Bedroom",
        "Classic Room Sea View": "Bedroom",
        "1 Bed in 2-bed dorm": "Bedroom",
        "Bed in 10-Bed Dormitory Room": "Bedroom",
        "Deluxe Female Dorm": "Bedroom",
        "Master Suite": "Bedroom",
        "Japanese Style Room": "Bedroom",
        "Twin Room or Double Room": "Bedroom",
        "Double Room without Window": "Bedroom",
        "Grand Spa Suite": "Bedroom",
        "2 Bedroom Apartment": "Bedroom",
        "Deluxe Sea View": "Bedroom",
        "Standard Double Deluxe Room": "Bedroom",
        "Double Room with Sea View": "Bedroom",
        "Privilege Double Room w or  sofa": "Bedroom",
        "Comfortable Room": "Bedroom",
        "Superior Room Casa e Mare Kata": "Bedroom",
        "Twin A": "Bedroom",
        "Classic Room": "Bedroom",
        "Garden View Twin": "Bedroom",
        "Garden Suite": "Bedroom",
        "5 Bedroom Villa": "Bedroom",
        "4 bed rooms private pool villa": "Bedroom",
        "6 Bedroom Pool Villa": "Bedroom",
        "Deluxe King Room": "Bedroom",
        "Standard Double (No window)": "Bedroom",
        "VIP Suite": "Bedroom",
        "4 Beds Mixed Dorm": "Bedroom",
        "Standard King A": "Bedroom",
        "Standard Double With Breakfast": "Bedroom",
        "Business King Bed Room": "Bedroom",
        "Deluxe King Bed": "Bedroom",
        "Standard King (includes 1 breakfast)": "Bedroom",
        "Standard King Bed": "Bedroom",
        "Deluxe Single": "Bedroom",
        "Single  or  Double": "Bedroom",
        "Suite View": "Bedroom",
        "Economy Twin": "Bedroom",
        "Single Room B - Smoking": "Bedroom",
        "Single Room B-Smoking": "Bedroom",
        "Deluxe Single Room - Smoking": "Bedroom",
        "Economy Double Room": "Bedroom",
        "Business Tiwn Room": "Bedroom",
        "Deluxe Suite": "Bedroom",
        "Superior King With Sea View": "Bedroom",
        "Business King Bed": "Bedroom",
        "Deluxe Garden View": "Bedroom",
        "Superior Room - Domestic Residents Only": "Bedroom",
        "Standard Kingsize": "Bedroom",
        "Twin Room with Breakfast (MiaoSi Room)": "Bedroom",
        "Cherry Blossoms Room": "Bedroom",
        "Business King Room In Main Building": "Bedroom",
        "2 Bedroom Villa": "Bedroom",
        "Triple - 1 Double Bed + 1 Single Bed": "Bedroom",
        "Family Room": "Bedroom",
        "Superior Queen Room": "Bedroom",
        "2 Double Bed Non Smoking": "Bedroom",
        "Superior Suite": "Bedroom",
        "Deluxe River View": "Bedroom",
        "Single Room A - Smoking": "Bedroom",
        "Japanese Room - Smoking": "Bedroom",
        "Royal Suite": "Bedroom",
        "Family Twin Room - Non-Smoking": "Bedroom",
        "Deluxe Room - Non-Smoking": "Bedroom",
        "Business Twin Room - Smoking": "Bedroom",
        "Panorama Room": "Bedroom",
        "1 Bedroom Deluxe Apartment": "Bedroom",
        "Standard Double or Twin Room": "Bedroom",
        "Superior Double or Twin": "Bedroom",
        "Bed in 11 Bed Mixed Dormitory Ensuite": "Bedroom",
        "Boutique King Room": "Bedroom",
        "Business Twin Bed": "Bedroom",
        "New Deluxe Twin Bed": "Bedroom",
        "Premium Japanese Suite": "Bedroom",
        "Single Room City": "Bedroom",
        "Ondol Room": "Bedroom",
        "Single Room Garden View with 1 Breakfast": "Bedroom",
        "Suite King": "Bedroom",
        "Luxury Room for 2 Guests": "Bedroom",
        "Standard Sea View Ground Floor With Breakfast": "Bedroom",
        "Glass sliding doors for nature view": "Bedroom",
        "1 Bed in 4-Bed Dormitory (Female)": "Bedroom",
        "Double Bed Ensuite with window": "Bedroom",
        "Superior Triple": "Bedroom",
        "Standard - Queen Bed": "Bedroom",
        "Ocean View Double": "Bedroom",
        "One Bedroom Villa Rice Field View": "Bedroom",
        "Rice Field View": "Bedroom",
        "Luxury Suite": "Bedroom",
        "Standard Family Room": "Bedroom",
        "Double - Must Check in before 7PM": "Bedroom",
        "Standard Window": "Bedroom",
        "Japanese Style Ocean View": "Bedroom",
        "Single A": "Bedroom",
        "Single-Smoking": "Bedroom",
        "Room with Tatami Area": "Bedroom",
        "Executive with breakfast": "Bedroom",
        "Ruby Room": "Bedroom",
        "Junior Room": "Bedroom",
        "3 Bedroom Pool Villa": "Bedroom",
        "Standard Twin Room": "Bedroom",
        "Standard Triple Room": "Bedroom",
        "Double Room with Share Bathroom": "Bedroom",
        "Studio Executive": "Bedroom",
        "Superior River View": "Bedroom",
        "Smart Queen Room": "Bedroom",
        "6 Bed Mixed Dormitory": "Bedroom",
        "Tasting room": "Bedroom",
        "Standard Small Double Smoking": "Bedroom",
        "Twin With Breakfast": "Bedroom",
        "Twin Room with Ocean View": "Bedroom",
        "Standard Twin Room - Non-Smoking": "Bedroom",
        "Japanese Style (Twin)": "Bedroom",
        "1 Bed in 6-Bed Dormitory": "Bedroom",
        "Park View Twin": "Bedroom",
        "3-Bedroom Villa": "Bedroom",

        "Dining": "Dining",
        "Restaurant": "Dining",
        "Outdoor Dining": "Dining",
        "Food and Drink": "Dining",
        "In-Room Dining": "Dining",
        "Buffet": "Dining",
        "Outdoor Banquet Area": "Dining",
        "Banquet Hall": "Dining",
        "Breakfast Area": "Dining",
        "Coffee Service": "Dining",
        "Snack Bar": "Dining",
        "Room Service - Dining": "Dining",
        "BBQ": "Dining",
        "Coffee Shop": "Dining",
        "Food Court": "Dining",
        "Family Dining": "Dining",
        "Sports Bar": "Dining",
        "Couples Dining": "Dining",
        "BBQ or Picnic Area": "Dining",
        "Private Kitchenette": "Dining",
        "Private Kitchen": "Dining",
        "Food and Beverages": "Dining",
        "Breakfast buffet": "Dining",
        "Breakfast Meal": "Dining",
        "Coffee and or or Coffee Maker": "Dining",
        "Ma Prow Restaurant": "Dining",
        "Terrace Restaurant": "Dining",
        "Coffee Shop or Cafe": "Dining",
        "Dining room": "Dining",
        "Shared Kitchen": "Dining",
        "On-site restaurant": "Dining",
        "Dining or Meeting Rooms": "Dining",
        "Main Restaurant": "Dining",
        "Multi Cuisine Restaurant": "Dining",
        "Edelweiss  The Restaurant": "Dining",
        "Mayuri-Multi Cuisine Restaurant": "Dining",
        "Temptation Bar and Restaurant": "Dining",
        "Papayon Cafe & Bistro": "Dining",
        "Food Fun (Multicuisine Restaurant)": "Dining",
        "Dawat - E - Khas": "Dining",
        "Nazare-Roof Top Restaurant cum Bar": "Dining",
        "Dining Hall": "Dining",
        "The Shells-Multi Cuisine Restaurant": "Dining",
        "Vegetarian Restaurant": "Dining",
        "Rice Bowl - Restaurant": "Dining",
        "Le Bonheur Villa Restaurant": "Dining",
        "The Summit - Rooftop Restaurant": "Dining",
        "Cafe Citra Rasa": "Dining",
        "Coffee Shop & Restaurant": "Dining",
        "Rose Cafe": "Dining",
        "Restaurant & Lobby": "Dining",
        "Ex-Libris Restaurant": "Dining",
        "Fonda Restaurant": "Dining",
        "Bar & restaurant ( on the beach )": "Dining",
        "Al Bustan Restaurant": "Dining",
        "Open Restaurant": "Dining",
        "Dayang Sumbi Coffee Shop": "Dining",
        "Ryokan Dining": "Dining",
        "D Room with Breakfast": "Dining",
        "Restaurant at 3rd Floor": "Dining",

        "Exterior": "Exterior",
        "Hotel Front": "Exterior",
        "Hotel Front - Evening": "Exterior",
        "Terrace": "Exterior",
        "View from Hotel": "Exterior",
        "Hotel Front - Evening or Night": "Exterior",
        "Lobby Sitting Area": "Exterior",
        "Terrace or Patio": "Exterior",
        "Exterior detail": "Exterior",
        "Courtyard": "Exterior",
        "Balcony View": "Exterior",
        "Aerial View": "Exterior",
        "Property Grounds": "Exterior",
        "Street View": "Exterior",
        "Balcony": "Exterior",
        "Sundeck": "Exterior",
        "Garden": "Exterior",
        "Birthday Party Area": "Exterior",
        "Boating": "Exterior",
        "Porch": "Exterior",
        "Lake": "Exterior",
        "Lake View": "Exterior",
        "City Shuttle": "Exterior",
        "Childrens Play Area - Outdoor": "Exterior",
        "Waterslide": "Exterior",
        "Outdoor Wedding Area": "Exterior",
        "Childrens Area": "Exterior",
        "Mini-Golf": "Exterior",
        "Beach": "Exterior",
        "Hotel Exterior": "Exterior",
        "Hotel Interior or Public Areas": "Exterior",
        "Fishing": "Exterior",
        "Airport Shuttle": "Exterior",
        "Parking": "Exterior",
        "Hotel Front - Evening/Night": "Exterior",
        "Indoor Golf Driving Range": "Exterior",
        "View from Property": "Exterior",
        "Front of Property": "Exterior",
        "Ski Hill": "Exterior",
        "Marina": "Exterior",
        "外观": "Exterior",
        "Beach/Ocean View": "Exterior",
        "Building design": "Exterior",
        "City View from Property": "Exterior",
        "Property Entrance": "Exterior",
        "Entrance": "Exterior",
        "Exterior view": "Exterior",
        "Floor plan": "Exterior",
        "Children’s Play Area - Outdoor": "Exterior",
        "Hunting": "Exterior",
        "Surroundings": "Exterior",
        "Ecotours": "Exterior",
        "Nearby attraction": "Exterior",
        "Safari": "Exterior",
        "Cottage Exterior": "Exterior",
        "Front view of Heaven Villa": "Exterior",
        "Golf Course": "Exterior",
        "Daytime Exterior": "Exterior",
        "HotelExterior": "Exterior",
        "Private beach": "Exterior",
        "Vineyard": "Exterior",
        "Sledding": "Exterior",

        "Gym": "Gym",
        "Fitness Facility": "Gym",
        "Sports Facility": "Gym",
        "Bicycling": "Gym",
        "Fitness Studio": "Gym",
        "Sauna": "Gym",
        "Steam Room": "Gym",
        "Aerobics Facility": "Gym",
        "Yoga": "Gym",
        "Basketball Court": "Gym",
        "Turkish Bath": "Gym",
        "Indoor Spa Tub": "Gym",
        "Vichy Shower": "Gym",
        "Rock Climbing Wall - Indoor": "Gym",
        "Outdoor Spa Tub": "Gym",
        "Sport Court": "Gym",
        "Tennis Court": "Gym",
        "Golf": "Gym",
        "Fitness & Recreational Facilities": "Gym",
        "Hiking": "Gym",
        "Outdoor Rock Climbing": "Gym",
        "Fitness center": "Gym",
        "Pilates": "Gym",
        "Ropes Course (Team Building)": "Gym",
        "Snow and Ski Sports": "Gym",
        "Skiing": "Gym",
        "健身娱乐设施": "Gym",
        "Kayaking": "Gym",
        "Snowboarding": "Gym",
        "Exercise": "Gym",
        "Sports and activities": "Gym",
        "Ice Skating": "Gym",
        "Fitness Room": "Gym",
        "Table tennis": "Gym",
        "tennis courts": "Gym",

        "In-Room Kitchen": "Kitchen",
        "Delicatessen": "Kitchen",
        "餐饮": "Kitchen",
        "Kitchen": "Kitchen",
        "Buffet Dim Sum": "Kitchen",
        "Empire's Kitchen": "Kitchen",
        "NOAH'S Kitchen": "Kitchen",
        "餐饮 or 会议": "Kitchen",

        "Hallway": "Lobby",
        "Lobby": "Lobby",
        "Lobby Lounge": "Lobby",
        "Executive Lounge": "Lobby",
        "Hotel Lobby": "Lobby",
        "Hostel Entrance": "Lobby",
        "Lobby area": "Lobby",
        "Lobby Seating": "Lobby",

        "Hotel Entrance": "Reception",
        "Reception": "Reception",
        "Reception Hall": "Reception",
        "Check-in or Check-out Kiosk": "Reception",
        "Hotel Reception Area": "Reception",
        "Reception area": "Reception",

        "Exercise or Lap Pool": "Swimming Pool",
        "Outdoor Pool": "Swimming Pool",
        "Rooftop Pool": "Swimming Pool",
        "Indoor Pool": "Swimming Pool",
        "Pool": "Swimming Pool",
        "Childrens Pool": "Swimming Pool",
        "Pool Waterfall": "Swimming Pool",
        "Water Park": "Swimming Pool",
        "Infinity Pool": "Swimming Pool",
        "Indoor or Outdoor Pool": "Swimming Pool",
        "Indoor/Outdoor Pool": "Swimming Pool",
        "Swimming pool [outdoor]": "Swimming Pool",
        "Natural Pool": "Swimming Pool",
        "Swimming pool": "Swimming Pool",
        "Hot springs": "Swimming Pool",
        "Public Bath": "Swimming Pool",
        "Exercise/Lap Pool": "Swimming Pool",
        "Children’s Pool": "Swimming Pool",
        "Hot tub": "Swimming Pool",
        "Swimming pool [kids]": "Swimming Pool",
        "Private Pool": "Swimming Pool",
        "Deluxe Pool View": "Swimming Pool",
        "Outdoor Swimming Pool": "Swimming Pool",
        "Roof top Swimming Pool": "Swimming Pool",
        "Poolside entrance": "Swimming Pool",
        "Pool and villa": "Swimming Pool",
        "Exterior pool": "Swimming Pool",
        "Pool Bar": "Swimming Pool",
        "hot spring bath": "Swimming Pool",
        "Pool Villa": "Swimming Pool",

        "Terrace/Patio": "Terrace",
        "Balcony or Terrace": "Terrace",
        "Roof top terrace": "Terrace",
        "Sylvan Terrace": "Terrace",
        "Rooftop terrace": "Terrace"
    }
    try:
        caption = list[caption]
    except KeyError:
        caption = 'Others'
        log(queue_id, "invalid caption", "caption", 1, caption)
    return caption
