Notes on Airbnb business in New York and elsewhere
==================================================

2014-08-26      Tom Slee

Version 2.1 is updated to be able to collect data from Airbnb's updated web
site. Not all cities have the new format, but the script should handle both 
versions. It will not, however, handle cities without neighborhoods.

2014-05-26      Tom Slee

Version 2 (May 2014) is much more thorough and efficient about searching
Airbnb's web site for a given city and has more options. I have moved it to
python 3 for better handling of unicode multi-lingual data. It is also ported
to SAP SQL Anywhere to allow more flexible reporting and better concurrency
than SQLite can provide. A free developer edition is available from the SAP web
site. 

- airbnb.py is the python script to collect data.
- plot.py just produces some charts.

airbnb.db is the data. The basic data is in the table *room*. A complete search of a given city's listings is a "survey" and the surveys are tracked in table *survey*. 

Using the script
----------------

Test that you can connect to the database: run =python airbnb.py -h= and
confirm that there are no errors. If there are errors, check the database
file setting near the top of the script and change its location.

To run a survey:
- add a city (search area) to the database, by running ./airbnb.py -ac
  "city-name". It scans the Airbnb web site and adds the neighborhoods for the
  city.
- add a survey to the database by running ./airbnb.py -as "city-name".
- collect the room_ids for the city by running ./airbnb.py -s survey_id. The
  survey_id can be seen by running ./airbnb -ls. This search loops over
  neighborhoods, property types, and pages of listings in the Airbnb search
  pages. 
- fill in the details of the rooms by running ./airbnb -f.


