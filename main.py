from sqlalchemy import Column, String, Integer, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from adops import util
import os
import pandas as pd
import datetime

# TODO:
# - don't forget to DROP processed table when working on this
# - add in check if file has been uploaded before


# Set up processed database
Base = declarative_base()
engine = create_engine('postgresql://blah@localhost:5432/reports')
engine2 = create_engine('postgresql://blah@localhost:5432/reports')


session = Session(bind=engine)

class Processed(Base):
    __tablename__ = 'processed'

    id = Column(Integer, primary_key=True)
    filehash = Column(String)
    filename = Column(String)
    row_count = Column(Integer)
    date_processed = Column(DateTime)
    report_type = Column(String)
    report_start_date = Column(DateTime)
    report_end_date = Column(DateTime)

    def __repr__(self):
        return "<File(%r)" % (self.filename)

Base.metadata.create_all(engine)


def append_column(df, value):
    row_count = df.shape[0]
    rows = []
    for x in xrange(row_count):
        rows.append(value)
    return rows


def import_to_sql(reports):
    for report in reports:
        df = pd.DataFrame()
        df = report.to_df(rename_cols=True)

        # Append Report Start Date:
        ser = append_column(df,report.start_date)
        start_series = pd.to_datetime(pd.Series(ser, name="report_start_date"))
        tmp_df = pd.concat([df, start_series], axis=1)

        # Append Report End Date:
        ser = append_column(df,report.end_date)
        end_series = pd.to_datetime(pd.Series(ser, name="report_end_date"))
        df = pd.concat([tmp_df, end_series], axis=1)

        filehash = report.githash()
        rows = len(df.index)
        date_processed = datetime.datetime.now()
        report_type = report.report_type
        print "Inserting %s rows for file: %s" % (rows, report.filename)

        # CHANGE VARIABLE
        df.to_sql('time_of_day', engine2, if_exists='append')

        process_transaction = Processed(filehash=filehash, filename=report.filename, row_count=rows,
                                        date_processed=date_processed, report_type=report.report_type,
                                        report_start_date=report.start_date,
                                        report_end_date=report.end_date)
        session.add(process_transaction)
        session.commit()




## if report_type == 'Site':
## if report_type == 'Site List':
## if report_type == 'Data Element Report':
## if report_type == 'Time of Day':
# if report_type == 'Browser Report':
# if report_type == 'Ad Group Recency':
# if report_type == 'Performance':
# if report_type == 'Geo Report':

current_directory = os.getcwd()
folder = os.path.join(current_directory, 'reports/')
reports = util.init_reports(folder)
filtered_reports = []
for rpt in reports:

    # CHANGE VARIABLE
    if rpt.report_type == 'Time of Day':
        filtered_reports.append(rpt)

import_to_sql(filtered_reports)