import os
import pandas as pd


def get_source_details(root_path):
    app_team_folders = []
    for (dirpath, dirnames, filenames) in os.walk(root_path):
        app_team_folders.extend(dirnames)
        break
    print(app_team_folders)
    input()


if __name__ == '__main__':
    QlikViewReportSourcePath = r'\\va125236\QlikView Reports\Application Teams'
    QlikViewReportUserPath = r'\\va125239\QlikView Reports\Application Teams'
    get_source_details(QlikViewReportSourcePath)
