import datetime
import os

import pandas as pd
import multiprocessing as mp
import time
import json

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
    folder_size = get_str_size(get_dir_size(folder_path))
    application_list = get_all_qvw_files(application_path)
    application_details = {}
    for app in application_list:
        application_details[app] = {}
        application_details[app]['modified_time'] = str(
            datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(application_path, app))))
        application_details[app]['size'] = get_str_size(os.path.getsize(os.path.join(application_path, app)))
        application_details[app]['file_path'] = os.path.join(application_path, app)
    generator_list = get_all_qvw_files(generator_path)
    generator_details = {}
    for app in generator_list:
        generator_details[app] = {}
        generator_details[app]['modified_time'] = str(
            datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(generator_path, app))))
        generator_details[app]['size'] = get_str_size(os.path.getsize(os.path.join(generator_path, app)))
        generator_details[app]['file_path'] = os.path.join(generator_path, app)
    qvd_list = get_all_qvd_files(qvd_path)
    qvd_details = {}
    for app in qvd_list:
        qvd_details[app] = {}
        qvd_details[app]['modified_time'] = str(
            datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(qvd_path, app))))
        qvd_details[app]['size'] = get_str_size(os.path.getsize(os.path.join(qvd_path, app)))
        qvd_details[app]['file_path'] = os.path.join(qvd_path, app)
    MainData[folder_name] = {'folder_name': folder_name, 'folder_path': folder_path, 'folder_size': folder_size,
                             'application_details': application_details,
                             'generator_details': generator_details, 'qvd_details': qvd_details}

    # print(MainData)
    return MainData[folder_name]


def df_analysis(json_data):
    columns = ['Folder Name', 'File Type', 'Last Modified', 'File Size', 'File Path']
    dt = []
    columns1 = ['Folder Name', 'Folder Size', 'Folder Size in bytes', 'No of Application Files',
                'No of Generator Files', 'No of QVD Files']
    dt1 = []
    for k, v in json_data.items():
        dt1.append([v['folder_name'], v['folder_size'], convert_back_to_bytes(v['folder_size']),
                    len(v['application_details'].keys()),
                    len(v['generator_details'].keys()), len(v['qvd_details'].keys())])
        for k1, v1 in v['application_details'].items():
            dt.append([v['folder_name'], 'Application', v1['modified_time'], v1['size'], v1['file_path']])
        for k1, v1 in v['generator_details'].items():
            dt.append([v['folder_name'], 'Generator', v1['modified_time'], v1['size'], v1['file_path']])
        for k1, v1 in v['qvd_details'].items():
            dt.append([v['folder_name'], 'QVD', v1['modified_time'], v1['size'], v1['file_path']])
    dframe = pd.DataFrame(dt)
    dframe.columns = columns
    dframe1 = pd.DataFrame(dt1)
    dframe1.columns = columns1
    return dframe1, dframe


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


def convert_back_to_bytes(size: str):
    term = ['bytes', 'KB', 'MB', 'GB', 'TB']
    sz, tm = size.split(' ')
    return int(int(sz) * (1024 ** term.index(tm)))


def get_dir_size(path: str):
    # data loss will occur
    if os.path.exists(path):
        size = 0
        for path, dirs, files in os.walk(path):
            for f in files:
                fp = os.path.join(path, f)
                size += os.path.getsize(fp)
        return size
    else:
        print('Path Not Found :', path)
        return 0


def get_str_size(size: int):
    term = ['bytes', 'KB', 'MB', 'GB', 'TB']
    lvl = 0
    while size > 1024:
        size /= 1024
        lvl += 1
    return str(round(size)) + ' ' + term[lvl]


def get_all_qvw_files(path: str):
    if os.path.exists(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.qvw')]
    else:
        print('Path Not Found :', path)
        return []


def get_all_qvd_files(path: str):
    if os.path.exists(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.qvd')]
    else:
        print('Path Not Found :', path)
        return []


def get_all_files(path: str):
    if os.path.exists(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    else:
        print('Path Not Found :', path)
        return []


def get_all_folders(path: str):
    if os.path.exists(path):
        return [f for f in os.listdir(path) if not os.path.isfile(os.path.join(path, f))]
    else:
        print('Path Not Found :', path)
        return []


if __name__ == '__main__':
    # get_source_details()
    start = time.time()
    app_team_folders = get_all_folders(QlikViewReportSourcePath)
    print(len(app_team_folders), 'folders found in source docs')
    pool = mp.Pool(processes=mp.cpu_count())
    print("Starting to Extract data from folders")
    res = pool.map(analyze_source_folder, app_team_folders)
    pool.close()
    pool.join()
    res = {i['folder_name']: i for i in res}
    # print(res)
    folder_details, complete_details = df_analysis(res)
    with pd.ExcelWriter('QVA.xlsx') as writer:
        folder_details.to_excel(writer, sheet_name='Overall Folder Details', index=False)
        complete_details.to_excel(writer, sheet_name='Complete Data', index=False)
    print('Analysis completed! Time taken :', round(time.time() - start, 2), 'seconds')
    with open('QV Analysis.json', 'w') as outfile:
        outfile.write(json.dumps(res, indent=4))
    print('Press to continue..')
    input()
