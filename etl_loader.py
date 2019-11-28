import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,scoped_session
from etl_model import ClientLocation
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LoaderDB():
    
    def load_etl_data(self,df):
        connect_string = 'mysql+pymysql://root:adminadmin@127.0.0.1:3306/etldb'
        sql_engine = create_engine(connect_string, pool_recycle=3600)
        ClientLocation.__table__.create(bind=sql_engine, checkfirst=True)
        Session = scoped_session(sessionmaker(bind=sql_engine))
        session = Session()
        
        Base.metadata.drop_all(sql_engine)
        Base.metadata.create_all(sql_engine)

        session.bulk_insert_mappings(ClientLocation, df.to_dict(orient="records"))
        session.commit()