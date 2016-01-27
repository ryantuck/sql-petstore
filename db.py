import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

class DsDb():

    def __init__(self,config):

        # get credentials for hitting databse
        user    = config['user']
        passwd  = config['passwd']
        host    = config['host']
        db      = config['db']
        port    = config['port']
        flavor  = config['flavor']

        # assemble location string
        location = flavor + '://'
        location += user + ':' + passwd
        location += '@' + host + ':' + str(port) + '/' + db

        # create relevant sqlalchemy stuff
        self.engine = create_engine(location)
        self.session = scoped_session(sessionmaker(bind=self.engine))
        self.base = declarative_base()

    def dataframe(self, query):

        """
            returns pandas dataframe from query
        """

        return pd.read_sql(query, self.engine)

    def list_from_query(self,query):

        """
            returns 1-D list for results
        """

        df = self.dataframe(query)
        return list(df[df.columns[0]])

    def df_to_sql(self, df, table):
        """
            appends dataframe to existing table
        """

        df.to_sql(
                table,
                self.engine,
                if_exists='append',
                index=False
                )








