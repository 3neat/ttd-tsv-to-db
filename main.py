from sqlalchemy import Column, String, Integer, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from adops import util
import os
import argparse
import pandas as pd
import datetime

# TODO:
# - don't forget to DROP processed table when working on this
# - add in check if file has been uploaded before


#Set up processed database
Base = declarative_base()
engine = create_engine()
engine2 = create_engine()

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


def import_to_sql(reports, dest_table):
    for report in reports:
        filehash = report.githash()

        if session.query(exists().where(Processed.filehash == filehash)).scalar():
            print "*** Already Processed: %s" % report.filename
        else:
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


            rows = len(df.index)
            date_processed = datetime.datetime.now()
            print "Inserting %s rows for file: %s" % (rows, report.filename)

            # CHANGE VARIABLE
            df.to_sql(dest_table, engine2, if_exists='append')

            process_transaction = Processed(filehash=filehash, filename=report.filename, row_count=rows,
                                            date_processed=date_processed, report_type=report.report_type,
                                            report_start_date=report.start_date,
                                            report_end_date=report.end_date)
            session.add(process_transaction)
            session.commit()


# (Error w/ recency bucket datatype conflict in "60+" string) if report_type == 'Ad Group Recency':

def main(tablename, reporttype, reportfolder='reports/'):
    current_directory = os.getcwd()
    folder = os.path.join(current_directory, reportfolder)
    reports = util.init_reports(folder)
    filtered_reports = []

    for rpt in reports:
        if rpt.report_type == reporttype:
            filtered_reports.append(rpt)

    import_to_sql(filtered_reports, tablename)

parser = argparse.ArgumentParser()
parser.add_argument("reporttype", help="define the report type to import to DB",
                    choices=["Site", "Site List", "Data Element Report", "Time of Day", "Performance", "Geo Report"])

parser.add_argument("tablename", help="import into this table name",
                    choices=["sites", "site_lists", "data_elements", "time_of_day", "performance", "geography"])
args = parser.parse_args()
main(args.tablename, args.reporttype)

