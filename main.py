import os
import pandas as pd
import multiprocessing as mp
import time

# input data
QlikViewReportSourcePath = r'\\va125236\QlikView Reports\Application Teams'
QlikViewReportUserPath = r'\\va125239\QlikView Reports\Application Teams'

MainData = {}


def analyze_source_folder(folder_name):
    folder_path = QlikViewReportSourcePath + '\\' + folder_name
    application_path = folder_path + '\\SourceDocuments\\6_Application'
    data_model_path = folder_path + '\\SourceDocuments\\5_DataModel'
    generator_path = folder_path + '\\SourceDocuments\\3_QVDGenerators'
    qvd_path = folder_path + '\\SourceDocuments\\4_QVD'
    resources_path = folder_path + '\\SourceDocuments\\1_Resources'
    qvscripts_path = folder_path + '\\SourceDocuments\\2_QVScripts'
    MainData[folder_name] = {'folder_name': folder_name, 'folder_path': folder_path, 'resources_path': resources_path,
                             'qvscripts_path': qvscripts_path, 'generator_path': generator_path,
                             'qvd_path': qvd_path, 'data_model_path': data_model_path,
                             'application_path': application_path}
    # print(MainData)
    return MainData[folder_name]


'''
def get_source_details():
    pool = mp.Pool(processes=mp.cpu_count())
    app_team_folders = get_all_folders(QlikViewReportSourcePath)
    print(app_team_folders)
    print("Starting to Extract data from folders")
    start = time.time()
    #for folder_name in app_team_folders:
    #    analyze_source_folder(folder_name)
        #res = pool.apply_async(analyze_source_folder, (folder_name,))
    pool.map(analyze_source_folder, app_team_folders)
    pool.close()
    pool.join()
    print(MainData)
'''


def get_all_folders(path):
    return [f for f in os.listdir(path) if not os.path.isfile(os.path.join(path, f))]


if __name__ == '__main__':
    # get_source_details()
    app_team_folders = get_all_folders(QlikViewReportSourcePath)
    print(app_team_folders)
    pool = mp.Pool(processes=mp.cpu_count())
    print("Starting to Extract data from folders")
    start = time.time()
    res = []
    for folder_name in app_team_folders:
        res.append(analyze_source_folder(folder_name))
    # res = pool.map(analyze_source_folder, app_team_folders)
    # pool.map(analyze_source_folder, app_team_folders)
    pool.close()
    pool.join()
    res = {i['folder_name']: i for i in res}
    print(res)

    print('Analysis completed! Time taken :', round(time.time() - start, 2), 'seconds')
