import datalog_parser
import pymysql.cursors
import pandas as pd

class MysqlWrapper(object):
  """Datalog Client for Mysql"""
  def __init__(self, host='dse203gtd.cvnmpos6almn.us-east-1.rds.amazonaws.com',
                     user='student',
                     password='LEbKqX3q',
                     db='gtd',
                     charset='utf8mb4', 
                     cursorclass=pymysql.cursors.DictCursor):
    super(MysqlWrapper, self).__init__()
    
    self.connection = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset, cursorclass=cursorclass)
    self.parser = datalog_parser.DatalogParser("asf")
    
  def execute(self, dl_query):
    sql_query = self.parser.parse(dl_query)
    print sql_query
    
    try:
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            rows = []
            for row in cursor:
                rows.append(row)
            
            return pd.DataFrame(rows)
    finally:
        self.connection.close()
    

  
if __name__ == '__main__':
  wrapper = MysqlWrapper()
  dl_query = "DB1(s1) :- gtd(_, 2013, 12, _, _, _, _, _,'Iraq', _, _, _, _, _, _, _, _, _, s1, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _,_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)"
  print dl_query
  print ""
  dataframe = wrapper.execute(dl_query)
  print dataframe