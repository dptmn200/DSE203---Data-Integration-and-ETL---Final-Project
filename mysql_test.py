import pandas as pd
import pymysql
from pandas.util.testing import assert_frame_equal
from mysql_wrapper import mysql_wrapper


def execute_test_sql(sql_query):
    host = 'dse203gtd.cvnmpos6almn.us-east-1.rds.amazonaws.com'
    user = 'student'
    password = 'LEbKqX3q'
    db = 'gtd'
    charset = 'utf8mb4'
    cursorclass = pymysql.cursors.DictCursor

    connection = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset,
                                 cursorclass=cursorclass)

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)
            rows = []
            for row in cursor:
                rows.append(row)

            return pd.DataFrame(rows)
    finally:
        connection.close()


def test_one():
    """Return the eventid's that occurred on 2012/12/25."""
    datalog_query = """q(eventid) :- gtd(eventid, iyear, imonth, iday, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,  _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,  _, _, _,
    _, _, _, _, _, _,_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,  _,
     _, _, _),iyear = 2012, imonth = 12, iday = 25"""

    wrapper = mysql_wrapper.MysqlWrapper()
    datalog_df = wrapper.execute(datalog_query)

    mysql_query = """select eventid
    from gtd.gtd g
    where iyear = 2012
    and imonth = 12
    and iday = 25"""

    mysql_df = execute_test_sql(mysql_query)

    assert_frame_equal(datalog_df, mysql_df)  # mysql_df.equals(datalog_df)


def test_two():
    """Return the summary of all events in the city of Khost, Afghanistan in 2012."""
    datalog_query = """q(summary) :- gtd(_, iyear, _, _, _, _, _, _, countrytxt, _, _, _, city, _, _, _, _, _, summary,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,  _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,  _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _), countrytxt = 'Afghanistan', city = 'Khost', iyear = 2012"""

    wrapper = mysql_wrapper.MysqlWrapper()
    datalog_df = wrapper.execute(datalog_query)

    mysql_query = """select summary
    from gtd.gtd g
    where country_txt = 'Afghanistan'
    and city = 'Khost'
    and iyear = 2012"""

    mysql_df = execute_test_sql(mysql_query)

    assert_frame_equal(datalog_df, mysql_df)  # mysql_df.equals(datalog_df)


def test_three():
    """Which countries had events on 2012-09-11?"""
    datalog_query = """q(country_txt) :- gtd(_, iyear, imonth, iday, _, _, _, _, country_txt, _, _, _, _, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _,  _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _,  _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _,  _, _, _, _), iday = 11, imonth = 9, iyear = 2012, DISTINCT """

    wrapper = mysql_wrapper.MysqlWrapper()
    datalog_df = wrapper.execute(datalog_query)

    mysql_query = """select distinct country_txt
    from gtd.gtd g
    where g.imonth = 9
    and g.iyear = 2012
    and g.iday = 11
    """

    mysql_df = execute_test_sql(mysql_query)

    assert_frame_equal(datalog_df, mysql_df)  # mysql_df.equals(datalog_df)


def test_four():
    """Return eventid and summary for all events with over 500 casualties."""
    datalog_query = """q(eventid,summary) :- gtd(eventid, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, summary,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, nkill, _, _, nwound, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _, _), nkill+nwound > 500"""

    wrapper = mysql_wrapper.MysqlWrapper()
    datalog_df = wrapper.execute(datalog_query)

    mysql_query = """select g.eventid, g.summary
    from gtd.gtd g
    where g.nkill + g.nwound > 500
    """

    mysql_df = execute_test_sql(mysql_query)

    assert_frame_equal(datalog_df, mysql_df)  # mysql_df.equals(datalog_df)


def test_five():
    """Which 5 days (month and day only) have the most attacks over all the years?"""
    datalog_query = """q(iday, imonth,num_events) :- gtd(eventid, _, imonth, iday, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _,  _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _,  _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,
    _, _, _, _, _, _, _, _, _, _, _), GROUP_BY([iday,imonth], num_events = COUNT(eventid)), SORT_BY(num_events, 'DESC'),
    LIMIT(5)"""

    wrapper = mysql_wrapper.MysqlWrapper()
    datalog_df = wrapper.execute(datalog_query)

    mysql_query = """select g.iday, g.imonth,count(eventid) num_events
    from gtd.gtd g
    group by g.iday, g.imonth
    order by count(eventid) desc
    limit 5;
    """

    mysql_df = execute_test_sql(mysql_query)

    assert_frame_equal(datalog_df, mysql_df)  # mysql_df.equals(datalog_df)
    
if __name__ == '__main__':
  test_one()
  test_two()
  test_three()
  test_four()
  test_five()
