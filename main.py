import os, json
from adops import report

def init_reports(folder):
    rpt = []
    for f in os.listdir(folder):
        if f.endswith(".tsv"):
            rpt.append(report.Report(os.path.join(folder, f)))
    return rpt


def main():
    current_directory = os.getcwd()
    folder = os.path.join(current_directory, 'reports/')
    reports = init_reports(folder)

    for report in reports:
        print report.filename



if __name__ == '__main__':
    main()