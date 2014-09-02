#!/usr/bin/python3
# ============================================================================
# Airbnb web site scraper, for analysis of Airbnb listings
# Tom Slee, 2013--2014
#
# function naming conventions:
#   ws_get = get from web site
#   db_get = get from database
#   db_add = add to the database
#
# usage conventions:
#   add = add to database
#   display = open a browser and show
#   list = get from database and print
#   print = get from web site and print
# ============================================================================
import re
import logging
import argparse
import sys
import traceback
import time
import random
import subprocess
#import codecs
#from cssselect import HTMLTranslator, SelectorError
import urllib.request
import urllib.parse
from lxml import html
import sqlanydb
import webbrowser
import os
#from PySide import QtGui
#from PySide import QtCore
#from PySide import QtWebKit


# CONSTANTS
URL_ROOM_ROOT = "http://www.airbnb.com/rooms/"
URL_HOST_ROOT = "https://www.airbnb.com/users/show/"
URL_SEARCH_ROOT = "http://www.airbnb.com/s/"
URL_TIMEOUT = 10.0
FILL_MAX_ROOM_COUNT = 50000
SEARCH_MAX_PAGES = 100
SEARCH_MAX_GUESTS = 16
FLAGS_ADD = 1
FLAGS_PRINT = 9
FLAGS_INSERT_REPLACE = True
FLAGS_INSERT_NO_REPLACE = False
DB_NAME = "dbnb"
DB_SERVERNAME = DB_NAME
DB_DIR = os.path.dirname(os.path.realpath(__file__)) + "/db"
DB_FILE = DB_DIR + "/" + DB_NAME + ".db"
MAX_CONNECTION_ATTEMPTS = 5

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
ch_formatter = logging.Formatter('%(levelname)-8s%(message)s')
console_handler.setFormatter(ch_formatter)
filelog_handler = logging.FileHandler("run.log")
filelog_handler.setLevel(logging.INFO)
fl_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
filelog_handler.setFormatter(fl_formatter)
logger.addHandler(console_handler)
logger.addHandler(filelog_handler)

# global database connection
try:
    conn = sqlanydb.connect(
        userid="DBA",
        password="sql",
        serverName=DB_SERVERNAME,
        databasename=DB_NAME,
        databasefile=DB_FILE)
except:
    logger.error(
        "Failed to connect to database." +
        "You may need to change the DB_FILE value in airbnb.py")


def list_search_area_info(search_area):
    try:
        cur = conn.cursor()
        cur.execute("""
                select search_area_id
                from search_area where name=?
                """, (search_area,))
        result_set = cur.fetchall()
        cur.close()
        count = len(result_set)
        if count == 1:
            print("\nThere is one search area called",
                  str(search_area),
                  "in the database.")
        elif count > 1:
            print("\nThere are", str(count),
                  "cities called", str(search_area),
                  "in the database.")
        elif count < 1:
            print("\nThere are no cities called",
                  str(search_area),
                  "in the database.")
            sys.exit()
        sql_neighborhood = """select count(*) from neighborhood
        where search_area_id = ? """
        sql_search_area = """select count(*) from search_area
        where search_area_id = ?"""
        for result in result_set:
            search_area_id = result[0]
            cur = conn.cursor()
            cur.execute(sql_neighborhood, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " neighborhoods.")
            cur = conn.cursor()
            cur.execute(sql_search_area, (search_area_id,))
            count = cur.fetchone()[0]
            cur.close()
            print("\t" + str(count) + " Airbnb cities.")
    except:
        logger.error("Failed to list search area info")
        raise


def list_surveys():
    try:
        cur = conn.cursor()
        cur.execute("""
            select survey_id, survey_date, survey_description, search_area_id
            from survey
            order by survey_id asc""")
        result_set = cur.fetchall()
        if len(result_set) > 0:
            template = "| {0:3} | {1:>10} | {2:>30} | {3:3} |"
            print(template.format("ID", "Date", "Description", "SA"))
            for survey in result_set:
                (id, date, desc, sa_id) = survey
                print(template.format(id, date, desc, sa_id))
    except:
        logger.error("Cannot list surveys.")
        raise


def list_room(room_id):
    try:
        columns = ('room_id', 'host_id', 'room_type', 'country',
                   'city', 'neighborhood', 'address', 'reviews',
                   'overall_satisfaction', 'accommodates',
                   'bedrooms', 'bathrooms', 'price',
                   'deleted', 'minstay', 'last_modified', 'latitude',
                   'longitude', 'survey_id', )
        sql = "select room_id"
        for column in columns[1:]:
            sql += ", " + column
        sql += " from room where room_id = ?"

        cur = conn.cursor()
        cur.execute(sql, (room_id,))
        result_set = cur.fetchall()
        if len(result_set) > 0:
            for result in result_set:
                i = 0
                print("Room information: ")
                for column in columns:
                    print("\t", column, "=", str(result[i]))
                    i += 1
            return True
        else:
            print("\nThere is no room", str(room_id), "in the database.\n")
            return False
        cur.close()
    except:
        traceback.print_exc(file=sys.stdout)
        raise


#class Render(QtWebKit.QWebPage):
#class Render():
#  def __init__(self, url):
#    #checks if QApplication already exists
#    self.app=QtGui.QApplication.instance()
#    #create QApplication if it doesnt exist
#    if not self.app:
#        self.app = QtGui.QApplication(sys.argv)
#    QtWebKit.QWebPage.__init__(self)
#    self.loadFinished.connect(self._loadFinished)
#    self.mainFrame().load(QtCore.QUrl(url))
#    self.app.exec_()

#  def _loadFinished(self, result):
#    self.frame = self.mainFrame()
#    self.app.quit()
def db_init():
    try:
        # get the directory this file is in
        if not os.path.isdir(DB_DIR):
            os.mkdir(DB_DIR)
        if not os.path.isfile(DB_FILE):
            logger.debug("dbiniting")
            subprocess.call(["dbinit", DB_FILE])
        subprocess.call(["dbisql",
                         "-nogui",
                         "-c",
                         "\"uid=dba;pwd=sql;" +
                         "dbf=" + DB_FILE +
                         ";eng=" + DB_SERVERNAME + "\"",
                         "read reload.sql"])
    except OSError:
        logger.error("Cannot create directory " + dbdir)
    except Exception as e:
        logger.error("Cannot create database file" + e.message )
        raise


def db_add_survey(search_area):
    try:
        cur = conn.cursor()
        cur.execute("call add_survey(?)", (search_area,))
        sql_identity = """select @@identity"""
        cur.execute(sql_identity, ())
        survey_id = cur.fetchone()[0]
        cur.execute("""select survey_id, survey_date,
        survey_description, search_area_id
        from survey where survey_id = ?""", (survey_id,))
        (survey_id,
         survey_date,
         survey_description,
         search_area_id) = cur.fetchone()
        conn.commit()
        cur.close()
        print("\nSurvey added:\n"
              + "\n\tsurvey_id=" + str(survey_id)
              + "\n\tsurvey_date=" + str(survey_date)
              + "\n\tsurvey_description=" + survey_description
              + "\n\tsearch_area_id=" + str(search_area_id))
    except:
        logger.error("Failed to add survey for " + search_area)
        raise


def db_get_neighborhoods_from_search_area(search_area_id):
    try:
        cur = conn.cursor()
        cur.execute("""
            select name
            from neighborhood
            where search_area_id =  ?
            order by name""", (search_area_id,))
        neighborhoods = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            neighborhoods.append(row[0])
        cur.close()
        return neighborhoods
    except:
        logger.error("Failed to retrieve neighborhoods from "
                     + str(search_area_id))
        raise


def db_get_search_area_info_from_db(search_area):
    try:
        # get city_id
        cur = conn.cursor()
        cur.execute("""
            select search_area_id
            from search_area
            where name = :search_area_name
            """, {"search_area_name": search_area})
        search_area_id = cur.fetchone()[0]
        print("\nFound search_area", search_area,
              ": search_area_id =", str(search_area_id))

        # get cities
        cur.execute("""select name
                       from city
                       where search_area_id = :search_area_id
                    """,
                    {"search_area_id": search_area_id})
        cities = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            cities.append(row[0])

        # get neighborhoods
        cur.execute("""
            select name
            from neighborhood
            where search_area_id =  :search_area_id
            """, {"search_area_id": search_area_id})
        neighborhoods = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            neighborhoods.append(row[0])

        cur.close()
        return (cities, neighborhoods)
    except:
        logger.error("Error getting search area info from db")
        raise


def db_get_room_to_fill():
    try:
        sql = """
        select room_id, survey_id
        from room
        where price is null and deleted != 1
        order by rand()
        """
        cur = conn.cursor()
        cur.execute(sql)
        (room_id, survey_id) = cur.fetchone()
        cur.close()
        return (room_id, survey_id)
    except TypeError:
        cur.close()
        logger.info("No unfilled rooms in database")
        raise
    except:
        logger.error("Error retrieving room to fill from db")
        cur.close()
        raise


def db_save_room_info(room_info, insert_replace_flag):
    try:
        logger.debug("In save_room_info for room " + str(room_info))
        if len(room_info) > 0:
            room_id = int(room_info[0])
        else:
            room_id = None
        deleted = room_info[13]
        cur = conn.cursor()
        try:
            if deleted == 1:
                sql = "update room set deleted = ? where room_id = ?"
                room_id = int(room_info[0])
                cur.execute(sql, (1, room_id,))
            else:
                sql = "insert into room "
                sql += """(
                    room_id,
                    host_id,
                    room_type,
                    country,
                    city,
                    neighborhood,
                    address,
                    reviews,
                    overall_satisfaction,
                    accommodates,
                    bedrooms,
                    bathrooms,
                    price,
                    deleted,
                    minstay,
                    latitude,
                    longitude,
                    survey_id
                    ) """
                if insert_replace_flag:
                    sql += "on existing update defaults on "
                sql += """
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cur.execute(sql, room_info)
            cur.close()
            conn.commit()
            logger.info("Saved room " + str(room_id))
            return
        except sqlanydb.IntegrityError:
            if insert_replace_flag:
                logger.error("Integrity error: " + str(room_id))
            else:
                logger.info("Listing already saved: " + str(room_id))
                pass   # not a problem
            cur.close()
        except ValueError as e:
            logger.error("room_id = " + str(room_id) + ": " + e.message)
            cur.close()
            raise
        except KeyboardInterrupt:
            raise
        except:
            cur.close()
            traceback.print_exc(file=sys.stdout)
            logger.error("Other error: " + str(room_id))
            raise
    except KeyboardInterrupt:
        raise
    except:
        logger.error("Error saving room")
        raise


def ws_get_page(url):
    # chrome gets the JavaScript-loaded content as well
    # see http://webscraping.com/blog/Scraping-JavaScript-webpages-with-webkit/
    #r = Render(url)
    #page = r.frame.toHtml()
    attempt = 0
    for attempt in range(5):
        try:
            response = urllib.request.urlopen(url, timeout=URL_TIMEOUT)
            page = response.read()
            break
        except KeyboardInterrupt:
            raise
        except:
            if attempt > MAX_CONNECTION_ATTEMPTS:
                logger.error("Probable connectivity problem retrieving "
                             "web page")
                # traceback.print_exc(file=sys.stdout)
                raise
    return page


def ws_get_room_info(room_id, survey_id, flag):
    try:
        # initialization
        logger.info("Getting info for room " + str(room_id)
                    + " from Airbnb web site")
        room_url = URL_ROOM_ROOT + str(room_id)
        page = ws_get_page(room_url)
        if page is not None:
            get_room_info_from_page(page, room_id, survey_id, flag)
            #logger.info(page)
            return True
        else:
            return False
    except BrokenPipeError as bpe:
        logger.error(bpe.message)
        raise
    except KeyboardInterrupt:
        logger.error("Keyboard interrupt")
        raise
    except:
        logger.error("Failed to get room " + str(room_id) + " from web site")
        raise


def get_room_info_from_page(page, room_id, survey_id, flag):
    #try:
        #print page
            #except UnicodeEncodeError:
        #if sys.version_info >= (3,):
            #logger.info(page.encode('utf8').decode(sys.stdout.encoding))
        #else:
            #logger.info(page.encode('utf8'))
            #print page.encode('utf8', 'replace')
    try:
        host_id = room_type = country = city = None
        neighborhood = address = reviews = overall_satisfaction = None
        accommodates = bedrooms = bathrooms = price = None
        latitude = longitude = None
        deleted = minstay = 1

        tree = html.fromstring(page)
        if tree is not None:
            deleted = 0

        # Some of these items do not appear on every page (eg,
        # ratings, bathrooms), and so their absence is marked with
        # logger.info. Others should be present for every room (eg,
        # latitude, room_type, host_id) and so are marked with a
        # warning.  Items coded in <meta
        # property="airbedandbreakfast:*> elements -- country --
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:country')]"
            "/@content"
            )
        if len(temp) > 0:
            country = temp[0]
        else:
            logger.info("No country found for room " + str(room_id))
        # -- city --
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:city')]"
            "/@content"
            )
        if len(temp) > 0:
            city = temp[0]
        else:
            logger.warning("No city found for room " + str(room_id))

        # -- rating --
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:rating')]"
            "/@content"
            )
        if len(temp) > 0:
            overall_satisfaction = temp[0]
        else:
            logger.info("No rating found for room " + str(room_id))
        # -- latitude --
        temp = tree.xpath("//meta"
                          "[contains(@property,"
                          "'airbedandbreakfast:location:latitude')]"
                          "/@content")
        if len(temp) > 0:
            latitude = temp[0]
        else:
            logger.warning("No latitude found for room " + str(room_id))
        # -- longitude --
        temp = tree.xpath(
            "//meta"
            "[contains(@property,'airbedandbreakfast:location:longitude')]"
            "/@content")
        if len(temp) > 0:
            longitude = temp[0]
        else:
            logger.warning("No longitude found for room " + str(room_id))

        # -- host_id --
        temp = tree.xpath(
            "//div[@id='host-profile']"
            "//a[contains(@href,'/users/show')]"
            "/@href"
        )
        if len(temp) > 0:
            host_id_element = temp[0]
            host_id_offset = len('/users/show/')
            host_id = int(host_id_element[host_id_offset:])
        else:
            temp = tree.xpath(
                "//div[@id='user']"
                "//a[contains(@href,'/users/show')]"
                "/@href")
            if len(temp) > 0:
                host_id_element = temp[0]
                host_id_offset = len('/users/show/')
                host_id = int(host_id_element[host_id_offset:])
            else:
                logger.warning("No host_id found for room " + str(room_id))

        # -- room type --
        temp = tree.xpath(
            "//div[@id='summary']"
            "//div[@class='panel-body']/div[@class='row'][2]"
            "/div[@class='col-9']"
            "//div[@class='col-3'][1]"
            "/text()"
            )
        if len(temp) > 0:
            room_type = temp[0].strip()
        else:
            # try old page match
            temp = tree.xpath(
                "//table[@id='description_details']"
                "//td[text()[contains(.,'Room type:')]]"
                "/following-sibling::td/text()")
            if len(temp) > 0:
                room_type = temp[0].strip()
            else:
                logger.warning("No room_type found for room " + str(room_id))

        # -- neighborhood --
        temp = tree.xpath(
            "//div[contains(@class,'rich-toggle')]/@data-address"
            )
        if len(temp) > 0:
            s = temp[0].strip()
            neighborhood = s[s.find("(")+1:s.find(")")]
        else:
            # try old page match
            temp = tree.xpath("//table[@id='description_details']"
                              "//td[text()[contains(.,'Neighborhood:')]]"
                              "/following-sibling::td/descendant::text()")
            if len(temp) > 0:
                neighborhood = temp[0].strip()
            else:
                logger.warning("No neighborhood found for room "
                               + str(room_id))

        # -- address --
        temp = tree.xpath(
            "//div[contains(@class,'rich-toggle')]/@data-address"
            )
        if len(temp) > 0:
            s = temp[0].strip()
            address = s[:s.find(",")]
        else:
            # try old page match
            temp = tree.xpath(
                "//span[@id='display-address']"
                "/@data-location"
                )
            if len(temp) > 0:
                address = temp[0]
            else:
                logger.info("No address found for room " + str(room_id))

        # -- reviews --
        temp = tree.xpath("//div[@id='room']/div[@id='reviews']//h4/text()")
        if len(temp) > 0:
            reviews = temp[0].strip()
            reviews = reviews.split('+')[0]
            reviews = reviews.split(' ')[0].strip()
            if reviews == "No":
                reviews = 0
        else:
            # try old page match
            temp = tree.xpath(
                "//span[@itemprop='reviewCount']/text()"
                )
            if len(temp) > 0:
                reviews = temp[0]
            else:
                logger.info("No reviews found for room " + str(room_id))

        # -- accommodates --
        temp = tree.xpath(
            "//div[@id='summary']"
            "//div[@class='panel-body']/div[@class='row'][2]"
            "/div[@class='col-9']"
            "//div[@class='col-3'][2]"
            "/text()"
            )
        if len(temp) > 0:
            accommodates = temp[0].strip()
            accommodates = accommodates.split('+')[0]
            accommodates = accommodates.split(' ')[0]
        else:
            # try old page match
            temp = tree.xpath("//table[@id='description_details']"
                              "//td[text()[contains(.,'Accommodates:')]]"
                              "/following-sibling::td/descendant::text()")
            if len(temp) > 0:
                accommodates = temp[0]
                accommodates = accommodates.split('+')[0]
            else:
                logger.warning("No accommodates found for room "
                               + str(room_id))

        # -- bedrooms --
        temp = tree.xpath(
            "//div[@id='summary']"
            "//div[@class='panel-body']/div[@class='row'][2]"
            "/div[@class='col-9']"
            "//div[@class='col-3'][3]"
            "/text()"
            )
        if len(temp) > 0:
            bedrooms = temp[0].strip()
            bedrooms = bedrooms.split('+')[0]
            bedrooms = bedrooms.split(' ')[0]
        else:
            # try old page match
            temp = tree.xpath(
                "//table[@id='description_details']"
                "//td[text()[contains(.,'Bedrooms:')]]"
                "/following-sibling::td/descendant::text()")
            if len(temp) > 0:
                bedrooms = temp[0].split('+')[0]
            else:
                logger.warning("No bedrooms found for room " + str(room_id))

        # -- bathrooms --
        temp = tree.xpath(
            "//div[@id='details-column']"
            "//div[text()[contains(.,'Bathrooms:')]]"
            "/strong/text()"
            )
        if len(temp) > 0:
            bathrooms = temp[0].split('+')[0]
        else:
            # try old page match
            temp = tree.xpath(
                "//table[@id='description_details']"
                "//td[text()[contains(.,'Bathrooms:')]]"
                "/following-sibling::td/descendant::text()"
                )
            if len(temp) > 0:
                bathrooms = temp[0].split('+')[0]
            else:
                logger.info("No bathrooms found for room " + str(room_id))

        # -- minimum stay --
        temp = tree.xpath(
            "//div[@id='details-column']"
            "//div[text()[contains(.,'Minimum Stay:')]]"
            "/strong/text()"
            )
        if len(temp) > 0:
            minstay = temp[0]
            non_decimal = re.compile(r'[^\d.]+')
            minstay = non_decimal.sub('', minstay)
        else:
            # try old page match
            temp = tree.xpath("//table[@id='description_details']"
                              "//td[text()[contains(.,'Minimum Stay:')]]"
                              "/following-sibling::td/descendant::text()")
            if len(temp) > 0:
                minstay = temp[0]
                non_decimal = re.compile(r'[^\d.]+')
                minstay = non_decimal.sub('', minstay)
            else:
                logger.info("No minstay found for room " + str(room_id))

        # -- price --
        temp = tree.xpath("//div[@id='price_amount']/text()")
        if len(temp) > 0:
            price = temp[0][1:]
            non_decimal = re.compile(r'[^\d.]+')
            price = non_decimal.sub('', price)
        else:
            # old page match is the same
            logger.info("No price found for room " + str(room_id))

        room_info = (
            room_id,        host_id,    room_type,
            country,        city,       neighborhood,
            address,        reviews,    overall_satisfaction,
            accommodates,   bedrooms,   bathrooms,
            price,          deleted,    minstay,
            latitude,       longitude,  survey_id
            )
        if len([x for x in room_info if x is not None]) < 6:
            logger.warn("Room " + str(room_id) + " has probably been deleted")
            #TODO better to make room_info a dict
            deleted = 1
            room_info = (
                room_id,        host_id,    room_type,
                country,        city,       neighborhood,
                address,        reviews,    overall_satisfaction,
                accommodates,   bedrooms,   bathrooms,
                price,          deleted,    minstay,
                latitude,       longitude,  survey_id
                )
        if flag == FLAGS_ADD:
            db_save_room_info(room_info, FLAGS_INSERT_REPLACE)
        elif flag == FLAGS_PRINT:
            print("Room info:")
            print("\troom_id:", str(room_id))
            print("\thost_id:", str(host_id))
            print("\troom_type:", room_type)
            print("\tcountry:", country)
            print("\tcity:", city)
            print("\tneighborhood:", neighborhood)
            print("\taddress:", address)
            print("\treviews:", reviews)
            print("\toverall_satisfaction:", overall_satisfaction)
            print("\taccommodates:", accommodates)
            print("\tbedrooms:", bedrooms)
            print("\tbathrooms:", bathrooms)
            print("\tprice:", price)
            print("\tdeleted:", deleted)
            print("\tlatitude:", str(latitude))
            print("\tlongitude:", str(longitude))
            print("\tminstay:", minstay)
        return True
    except KeyboardInterrupt:
        raise
    except IndexError:
        logger.error("Web page has unexpected structure.")
        raise
    except:
        logger.error("Error parsing web page")
        raise


def display_room(room_id):
    webbrowser.open(URL_ROOM_ROOT + str(room_id))


def display_host(host_id):
    webbrowser.open(URL_HOST_ROOT + str(host_id))


def fill_loop_by_room():
    try:
        room_count = 0
        while room_count < FILL_MAX_ROOM_COUNT:
            (room_id, survey_id) = db_get_room_to_fill()
            if room_id is None:
                break
            else:
                time.sleep(3.0 * random.random())
                if(ws_get_room_info(room_id, survey_id, FLAGS_ADD)):
                    room_count += 1
    except TypeError as te:
        raise
    except:
        logger.error("Error in fill_loop_by_room")
        raise


def db_get_search_area_from_survey_id(survey_id):
    try:
        cur = conn.cursor()
        cur.execute("""
            select sa.search_area_id, sa.name
            from search_area sa join survey s
            on sa.search_area_id = s.search_area_id
            where s.survey_id = ?""", (survey_id,))
        (search_area_id, name) = cur.fetchone()
        cur.close()
        return (search_area_id, name)
    except KeyboardInterrupt:
        cur.close()
        raise
    except:
        cur.close()
        logger.error("No search area for survey_id" + str(survey_id))
        raise


def page_has_been_retrieved(survey_id, room_type, neighborhood, guests,
                            page_number):
    """
    Returns 1 if the page has been retrieved and has rooms
    Returns 0 if the page has been retrieved and has no rooms
    Returns -1 if the page has not been retrieved
    """
    cur = conn.cursor()
    count = 0
    try:
        sql = """
            select ssp.has_rooms
            from survey_search_page ssp
            join neighborhood nb
            on ssp.neighborhood_id = nb.neighborhood_id
            where survey_id = ?
            and room_type = ?
            and nb.name = ?
            and guests = ?
            and page_number = ?"""
        cur.execute(sql, (survey_id, room_type, neighborhood, guests,
                          page_number))
        count = cur.fetchone()[0]
        logger.debug("count = " + str(count))
    except:
        count = -1
        logger.debug("page has not been retrieved previously")
    finally:
        cur.close()
        return count


def db_save_survey_search_page(survey_id, room_type, neighborhood_id,
                               guests, page_number, has_rooms):
    try:
        sql = """
        insert into survey_search_page(survey_id, room_type, neighborhood_id,
        guests, page_number, has_rooms)
        values (?, ?, ?, ?, ?, ?)
        """
        cur = conn.cursor()
        cur.execute(sql, (survey_id, room_type, neighborhood_id, guests,
                          page_number, has_rooms))
        cur.close()
        conn.commit()
        return True
    except:
        logger.error("Save survey search page failed")
        return False


def db_get_neighborhood_id(survey_id, neighborhood):
    sql = """
    select neighborhood_id
    from neighborhood nb
        join search_area sa
        join survey s
    on nb.search_area_id = sa.search_area_id
    and sa.search_area_id = s.search_area_id
    where s.survey_id = ?
    and nb.name = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (survey_id, neighborhood, ))
    neighborhood_id = cur.fetchone()[0]
    cur.close()
    return neighborhood_id


def search_page_url(search_area_name, guests, neighborhood, room_type,
                    page_number):
    url_root = URL_SEARCH_ROOT + search_area_name
    url_suffix = "guests=" + str(guests)
    url_suffix += "&"
    url_suffix += urllib.parse.quote("neighborhoods[]")
    url_suffix += "="
    # Rome: Unicode wording, equal comparison failed
    # to convert both args to unicode (prob url_suffix
    # and urllib2.quote(neighborhood)
    url_suffix += urllib.parse.quote(neighborhood)
    url_suffix += "&"
    url_suffix += urllib.parse.quote("room_types[]")
    url_suffix += "="
    url_suffix += urllib.parse.quote(room_type)
    url_suffix += "&"
    url_suffix += "page=" + str(page_number)
    url = url_root + "?" + url_suffix
    logger.debug("URL: " + url)
    return url


def ws_get_search_page_info(survey_id, search_area_name, room_type,
                            neighborhood, guests, page_number, flag):
    logger.info(
        room_type + ", " +
        neighborhood + ", " +
        str(guests) + " guests, " +
        "page " + str(page_number))
    url = search_page_url(search_area_name, guests, neighborhood, room_type,
                          page_number)
    time.sleep(3.0 * random.random())
    page = ws_get_page(url)
    if page is False:
        return 0
    tree = html.fromstring(page)
    room_elements = tree.xpath(
        "//div[@class='listing']/@data-id"
    )
    logger.debug("Found " + str(len(room_elements)) + " rooms.")
    neighborhood_id = db_get_neighborhood_id(
        survey_id, neighborhood)
    room_count = len(room_elements)
    if room_count > 0:
        has_rooms = 1
    else:
        has_rooms = 0
    if flag == FLAGS_ADD:
        db_save_survey_search_page(survey_id, room_type,
                                   neighborhood_id, guests,
                                   page_number, has_rooms)
    if room_count > 0:
        for room_element in room_elements:
            room_id = int(room_element)
            if room_id is not None:
                room_info = (
                    room_id,
                    None,  # host_id,
                    room_type,  # room_type,
                    None,  # country,
                    None,  # city,
                    None,  # neighborhood,
                    None,  # address,
                    None,  # reviews,
                    None,  # overall_satisfaction
                    None,  # accommodates
                    None,  # bedrooms
                    None,  # bathrooms
                    None,  # price
                    0,     # deleted
                    None,  # minstay
                    None,  # latitude
                    None,  # longitude
                    survey_id,  # survey_id
                    )
                if flag == FLAGS_ADD:
                    db_save_room_info(room_info, FLAGS_INSERT_NO_REPLACE)
                elif flag == FLAGS_PRINT:
                    print(room_info[2], room_info[0])
    else:
        logger.info("No rooms found")
    return room_count


def searcher(survey_id, flag):
    try:
        (search_area_id, search_area_name) = \
            db_get_search_area_from_survey_id(survey_id)
        neighborhoods = db_get_neighborhoods_from_search_area(search_area_id)
        for room_type in (
                "Private room",
                "Entire home/apt",
                "Shared room",
                ):
            logger.debug("Searching for %(rt)s" % {"rt": room_type})
            for neighborhood in neighborhoods:
                if room_type in ("Private room", "Shared room"):
                    max_guests = 4
                else:
                    max_guests = SEARCH_MAX_GUESTS
                for guests in range(1, max_guests):
                    for page_number in range(1, SEARCH_MAX_PAGES):
                        if flag != FLAGS_PRINT:
                            # for FLAGS_PRINT, fetch one page and print it
                            count = page_has_been_retrieved(
                                survey_id, room_type,
                                neighborhood, guests, page_number)
                            if count == 1:
                                logger.debug("\t...page already visited")
                                continue
                            elif count == 0:
                                logger.debug("\t...page already visited")
                                break
                            else:
                                logger.debug("\t...visiting page")
                        room_count = ws_get_search_page_info(
                            survey_id,
                            search_area_name,
                            room_type,
                            neighborhood,
                            guests,
                            page_number,
                            flag)
                        if room_count <= 0:
                            break
                        if flag == FLAGS_PRINT:
                            return
    except KeyboardInterrupt:
        raise
    except:
        logger.error("Error while searching")
        raise


def ws_get_city_info(city, flag):
    try:
        url = URL_SEARCH_ROOT + city
        page = ws_get_page(url)
        if page is False:
            return False
        tree = html.fromstring(page)
        try:
            citylist = tree.xpath(
                "//input[@name='location']/@value")
            neighborhoods = tree.xpath(
                "//input[@name='neighborhood']/@value")
            if flag == FLAGS_PRINT:
                print("\n", citylist[0])
                print("Neighborhoods:")
                for neighborhood in neighborhoods:
                    print("\t", neighborhood)
            elif flag == FLAGS_ADD:
                if len(citylist) > 0:
                    cur = conn.cursor()
                    # check if it exists
                    sql_check = """
                        select name
                        from search_area
                        where name = ?"""
                    cur.execute(sql_check, (citylist[0],))
                    if cur.fetchone() is not None:
                        logger.info("City already exists: " + citylist[0])
                        return
                    sql_search_area = """insert
                                into search_area (name)
                                values (?)"""
                    cur.execute(sql_search_area, (citylist[0],))
                    #city_id = cur.lastrowid
                    sql_identity = """select @@identity"""
                    cur.execute(sql_identity, ())
                    search_area_id = cur.fetchone()[0]
                    sql_city = """insert
                            into city (name, search_area_id)
                            on existing skip
                            values (?,?)"""
                    cur.execute(sql_city, (city, search_area_id,))
                    logger.info("Added city " + city)
                    logger.debug(str(len(neighborhoods)) + " neighborhoods")
                if len(neighborhoods) > 0:
                    sql_neighborhood = """
                        insert
                        into neighborhood(name, search_area_id)
                        values(?, ?)
                        """
                    for neighborhood in neighborhoods:
                        cur.execute(sql_neighborhood, (neighborhood,
                                                       search_area_id,))
                        logger.info("Added neighborhood " + neighborhood)
                else:
                    logger.info("No neighborhoods found for " + city)
                conn.commit()
        except UnicodeEncodeError:
            #if sys.version_info >= (3,):
            #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
            #else:
            #    logger.info(s.encode('utf8'))
            # unhandled at the moment
            pass
        except:
            logger.error("Error collecting city and neighborhood information")
            raise
    except:
        logger.error("Error getting city info from website")
        raise


def main():
    parser = \
        argparse.ArgumentParser(
            description='Manage a database of Airbnb listings.',
            usage='%(prog)s [options]')
    # Only one argument!
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-asa', '--addsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""get and save the name and neighborhoods
                       for search area (city)""")
    group.add_argument('-ar', '--addroom',
                       metavar='room_id', action='store', default=False,
                       help='add a room_id to the database')
    group.add_argument('-asv', '--addsurvey',
                       metavar='search_area', type=str,
                       help="""add a survey entry to the database,
                       for search_area""")
    group.add_argument('-dbi', '--dbinit',
                       action='store_true', default=False,
                       help='Initialize the database file')
    group.add_argument('-dh', '--displayhost',
                       metavar='host_id', type=int,
                       help='display web page for host_id in browser')
    group.add_argument('-dr', '--displayroom',
                       metavar='room_id', type=int,
                       help='display web page for room_id in browser')
    group.add_argument('-f', '--fill',
                       action='store_true', default=False,
                       help='fill in details for room_ids collected with -s')
    group.add_argument('-lsa', '--listsearcharea',
                       metavar='search_area', type=str,
                       help="""list information about this search area
                       from the database""")
    group.add_argument('-lr', '--listroom',
                       metavar='room_id', type=int,
                       help='list information about room_id from the database')
    group.add_argument('-ls', '--listsurveys',
                       action='store_true', default=False,
                       help='list the surveys in the database')
    group.add_argument('-psa', '--printsearcharea',
                       metavar='search_area', action='store', default=False,
                       help="""print the name and neighborhoods for
                       search area (city) from the Airbnb web site""")
    group.add_argument('-pr', '--printroom',
                       metavar='room_id', type=int,
                       help="""print room_id information
                       from the Airbnb web site""")
    group.add_argument('-ps', '--printsearch',
                       metavar='survey_id', type=int,
                       help="""print first page of search information
                       for survey from the Airbnb web site""")
    group.add_argument('-s', '--search',
                       metavar='survey_id', type=int,
                       help='search for rooms using survey survey_id')
    group.add_argument('-v', '--version',
                       action='version',
                       version='%(prog)s, version 2.1')
    group.add_argument('-?', action='help')

    args = parser.parse_args()

    try:
        if args.search:
            searcher(args.search, FLAGS_ADD)
        elif args.fill:
            fill_loop_by_room()
        elif args.addsearcharea:
            ws_get_city_info(args.addsearcharea, FLAGS_ADD)
        elif args.addroom:
            ws_get_room_info(int(args.addroom), None, FLAGS_ADD)
        elif args.addsurvey:
            db_add_survey(args.addsurvey)
        elif args.dbinit:
            db_init()
        elif args.displayhost:
            display_host(args.displayhost)
        elif args.displayroom:
            display_room(args.displayroom)
        elif args.listsearcharea:
            list_search_area_info(args.listsearcharea)
        elif args.listroom:
            list_room(args.listroom)
        elif args.listsurveys:
            list_surveys()
        elif args.printsearcharea:
            ws_get_city_info(args.printsearcharea, FLAGS_PRINT)
        elif args.printroom:
            ws_get_room_info(args.printroom, None, FLAGS_PRINT)
        elif args.printsearch:
            #page = ws_get_search_page(url)
            searcher(args.printsearch, FLAGS_PRINT)
        else:
            parser.print_help()
    except KeyboardInterrupt:
        sys.exit()
    except:
        #traceback.print_exc(file=sys.stdout)
        sys.exit()

if __name__ == "__main__":
    main()
