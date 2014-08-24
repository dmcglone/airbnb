#!/usr/bin/python
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib
import numpy as np
import sys, traceback
import logging
import sqlanydb as db

logging.basicConfig(format='%(message)s',
                    level=logging.INFO)
conn = db.connect(
	userid="DBA",
	password="sql",
	serverName="airbnb",
	databasename="airbnb",
	databasefile="/home/tom/src/airbnb/db/airbnb.db",
)

PIECHART_EXPLODE=0.05

class byhost:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        count(*) hosts
        from survey_host(@survey_id)
        group by host_type
        order by host_type desc
        """
    name = "host"
    names = "hosts"
    ylabel = names.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " Profile"
    filename = "@survey_description_" + name + ".pdf"

class byhost_with_reviews:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        count(*) hosts
        from survey_host(@survey_id)
        where revs > 0
        group by host_type
        order by host_type desc
        """
    name = "host"
    names = "hosts"
    ylabel = names.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " With Reviews Profile"
    filename = "@survey_description_" + name + "_withreviews.pdf"

class bylisting:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        sum(rooms) listings
        from survey_host(@survey_id)
        group by host_type
        order by host_type desc;
        """
    name = "listing"
    names = "listings"
    ylabel = names.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " Profile"
    filename = "@survey_description_" + name + ".pdf"

class bybooking:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        sum(revs) bookings
        from survey_host(@survey_id)
        group by host_type
        order by host_type desc;
        """
    name = "booking"
    names = "bookings"
    ylabel = names.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " Profile"
    filename = "@survey_description_" + name + ".pdf"

class byincome:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        sum(income1) income
        from survey_host(@survey_id)
        group by host_type
        order by host_type desc;
        """
    name = "income"
    names = "dollars"
    ylabel = name.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " Profile"
    filename = "@survey_description_" + name + ".pdf"

class byincome2:
    sql = """
        select
        case
            when multilister > 0.5 then 'Multiple'
            else 'Single'
            end as host_type,
        sum(income2) income
        from survey_host(@survey_id)
        group by host_type
        order by host_type desc;
        """
    name = "income_minstay"
    names = "dollars"
    ylabel = name.title() + " (%)"
    xlabel = "Listings per host"
    title = name.title() + " Profile"
    filename = "@survey_description_" + name + ".pdf"

class listings_byroomtype:
    sql = """
        select
        room_type,
        count(*) listings
        from survey_room(@survey_id)
        group by room_type
        order by room_type
        """
    name = "listing"
    names = "listings"
    xlabel = "Room Type"
    ylabel = name.title() + " (%)"
    title = names.title() + " By Room Type"
    filename = "@survey_description_" + names + "_byroomtype.pdf"

class bookings_byroomtype:
    sql = """
        select
        room_type,
        sum (reviews) bookings
        from survey_room(@survey_id)
        group by room_type
        order by room_type
        """
    name = "booking"
    names = "bookings"
    xlabel = "Room Type"
    ylabel = name.title() + " (%)"
    title = names.title() + " By Room Type"
    filename = "@survey_description_" + names + "_byroomtype.pdf"

class income1_byroomtype:
    sql = """
        select
        room_type,
        sum (reviews*price) income_1
        from survey_room(@survey_id)
        group by room_type
        order by room_type
        """
    name = "income"
    names = "income"
    xlabel = "Room Type"
    ylabel = name.title() + " (%)"
    title = name.title() + " By Room Type"
    filename = "@survey_description_" + names + "1_byroomtype.pdf"

class income2_byroomtype:
    sql = """
        select
        room_type,
        sum (reviews*price*minstay) income_2
        from survey_room(@survey_id)
        group by room_type
        order by room_type
        """
    name = "income"
    names = "income"
    xlabel = "Room Type"
    ylabel = name.title() + " (%)"
    title = name.title() + " By Room Type"
    filename = "@survey_description_" + names + "2_byroomtype.pdf"

class rating:
    sql = """
        select
        overall_satisfaction rating,
        count(*) n
        from survey_room(@survey_id)
        where rating is not null
        group by rating
        order by rating asc
        """
    name = "listing"
    names = "listings"
    xlabel = "Rating"
    ylabel = names.title() + " (%)"
    title = xlabel.title() + " Distribution"
    filename = "@survey_description_" + names + "_listings_by_rating.pdf"

def piechart(plotter, survey_id, survey_description):
    try:
        sql = plotter.sql.replace('@survey_id', str(survey_id))
        logging.debug(sql)
        c = conn.cursor()
        c.execute(sql)
        result_set = c.fetchall()
        (labels,fractions, ) = ([x for x,y in result_set],
                                [float(y) for x,y in result_set])
        # convert values to percentages
        total = sum(fractions)
        fractions = [(yi*100.0/total) for yi in fractions]
        explode = [ PIECHART_EXPLODE if y < 25 else 0.0 for y in fractions ]
        filename = plotter.filename.replace(
                '@survey_description', survey_description)
        filename = filename.replace(' ', '_')

        plt.ioff() # turn off interactive
        plt.clf()  # clear figure of previous plots
	plt.axes(aspect=1)
        plt.title(survey_description.title() + ": " + plotter.title,
        fontsize='x-large', fontweight='bold')
        patches, texts, autotexts = plt.pie(fractions,
		labels=labels,
                explode=explode,
		colors=('lightsteelblue', 'lightsalmon',
                    'lightyellow','lightseagreen','lightslategray',),
		shadow=False,
		autopct='%1.1f%%',
		)

 	font_properties = fm.FontProperties()
	font_properties.set_size('x-large')
	plt.setp(autotexts, fontproperties=font_properties)
	plt.setp(texts, fontproperties=font_properties)

        plt.savefig("./img/pi_" + filename, bbox_inches='tight' )
        c.close()
        print "Pie chart of ", plotter.names, "from ", survey_description, "saved to", filename

    except KeyboardInterrupt:
        sys.exit()
    except:
        traceback.print_exc(file=sys.stdout)

def plot(plotter, survey_id, survey_description):
    try:
        sql = plotter.sql.replace("@survey_id", str(survey_id))
        c = conn.cursor()
        c.execute(sql)
        result_set = c.fetchall()

        filename = plotter.filename.replace(
                "@survey_description", survey_description)
        filename = filename.replace(' ', '_')
        bar_width = 0.25
        bar_color = 'steelblue'
        edge_color = 'black'
        opacity = 0.8
        (x,y) = ([x for x,y in result_set],
                 [float(y) for x,y in result_set])
        # convert values to percentages
        total = sum(y)
        y = [(yi*100.0/total) for yi in y]
        index = xrange(len(result_set))

        plt.ioff() # turn off interactive
        plt.clf()  # clear figure of previous plots
        plt.tick_params('x', length=0, labelbottom = True )
        rectangles = plt.bar(index, y, 2.0*bar_width, alpha=opacity,
                color=bar_color, edgecolor=edge_color,
                linewidth=1, align='center')
        # label values at the top
        for i,rect in enumerate(rectangles):
            height = rect.get_height()
            height = height + 2.0
            plt.text(rect.get_x()+rect.get_width()/2.,
                    height, '%s'% (str(y[i])[0:4] + "%"),
                    ha='center', va='bottom')

        # plt.text(0.1, 90, str(total) + " " + plotter.__name__ + " in " + city)
        plt.grid(False)
        plt.xlabel(plotter.xlabel)
        plt.ylabel(plotter.ylabel)
        plt.xticks(index, x)
        plt.ylim(0.0, 100.0)
        plt.title(city.title() + ": " + plotter.title)
        # these are matplotlib.patch.Patch properties
        props = dict(boxstyle='square,pad=0.5', facecolor='wheat', alpha=0.5)
        if plotter.name != "income":
            plt.text(0.9, 0.9,
                str(int(total)) + " " + plotter.names,
                transform=plt.gca().transAxes,
                horizontalalignment='right',
                verticalalignment='top',
                bbox=props)
        plt.legend()
        plt.tight_layout()
        plt.savefig("./img/" + filename)
        c.close()
        print "Plot", plotter.names, "in", city, "saved to", filename
    except KeyboardInterrupt:
        sys.exit()
    except:
        traceback.print_exc(file=sys.stdout)

def main():
    sql = """
    SELECT survey_id, survey_description
    FROM survey
    """
    c = conn.cursor()
    c.execute(sql)
    result_set = c.fetchall()
    (survey_ids,survey_descriptions) = (
            [survey_id for survey_id,survey_description in result_set],
            [survey_description for survey_id,survey_description in result_set]
            )
    for result in result_set:
        (survey_id, survey_description) = result
        piechart(byhost, survey_id, survey_description)
        piechart(byhost_with_reviews, survey_id, survey_description)
        piechart(bylisting, survey_id, survey_description)
        piechart(bybooking, survey_id, survey_description)
        piechart(byincome, survey_id, survey_description)
        piechart(byincome2, survey_id, survey_description)
        piechart(listings_byroomtype, survey_id, survey_description)
        piechart(bookings_byroomtype, survey_id, survey_description)
        #piechart(income1_byroomtype, survey_id, survey_description)
        #piechart(income2_byroomtype, survey_id, survey_description)
        # plot(rating, survey_id)

if __name__ == "__main__":
    main()

