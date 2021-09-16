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


def get_data_from_folder_path(path: str):
    master = {}
    if os.path.exists(path):
        files_and_folders = os.listdir(path)
        for i in files_and_folders:
            if os.path.isfile(os.path.join(path, i)):
                master[i] = get_meta_details_from_file(path, i)
            else:
                master.update(get_data_from_folder_path(os.path.join(path, i)))
        return master
    else:
        print('Path Not Found :', path)
        return {}


def analyze_source_folder(folder_name, source_path, user_path):
    folder_path = source_path + '\\' + folder_name
    application_path = folder_path + '\\SourceDocuments\\6_Application'
    data_model_path = folder_path + '\\SourceDocuments\\5_DataModel'
    generator_path = folder_path + '\\SourceDocuments\\3_QVDGenerators'
    qvd_path = folder_path + '\\SourceDocuments\\4_QVD'
    resources_path = folder_path + '\\SourceDocuments\\1_Resources'
    qvscripts_path = folder_path + '\\SourceDocuments\\2_QVScripts'
    user_application_path = user_path + '\\' + folder_name + '\\UserDocuments'
    user_app_file_list = get_all_qvw_files(user_application_path)
    folder_size = get_str_size(get_dir_size(folder_path))
    application_list = get_all_qvw_files(application_path)
    user_only_application_details = {}
    for user_app in user_app_file_list:
        if user_app in application_list:
            user_only_application_details[user_app] = get_meta_details_from_file(user_application_path, user_app)
    application_details = {}
    for app in application_list:
        application_details[app] = get_meta_details_from_file(application_path, app)
    generator_list = get_all_qvw_files(generator_path)
    generator_details = {}
    for app in generator_list:
        generator_details[app] = get_meta_details_from_file(generator_path, app)
    qvd_list = get_all_qvd_files(qvd_path)
    qvd_details = {}
    for app in qvd_list:
        qvd_details[app] = get_meta_details_from_file(qvd_path, app)
    data_model_details = get_data_from_folder_path(data_model_path)
    resources_details = get_data_from_folder_path(resources_path)
    qvscripts_details = get_data_from_folder_path(qvscripts_path)
    MainData[folder_name] = {'folder_name': folder_name, 'folder_path': folder_path, 'folder_size': folder_size,
                             'application_details': application_details,
                             'generator_details': generator_details, 'qvd_details': qvd_details,
                             'user_only_application_details': user_only_application_details,
                             'data_model_details': data_model_details, 'qvscripts_details': qvscripts_details,
                             'resources_details': resources_details}
    return MainData[folder_name]


def df_analysis(json_data):
    columns = ['Folder Name', 'File Name', 'File Type', 'Last Modified', 'File Size', 'File Path', ]
    dt = []
    columns1 = ['Folder Name', 'Folder Size', 'Folder Size in bytes', '# Applications', '# User-Only Application',
                '# Generator Files', '# QVD']
    dt1 = []
    dt2 = []  # unused items for past 6 months
    for k, v in json_data.items():
        dt1.append([v['folder_name'], v['folder_size'], convert_back_to_bytes(v['folder_size']),
                    len(v['application_details'].keys()), len(v['user_only_application_details'].keys()),
                    len(v['generator_details'].keys()), len(v['qvd_details'].keys())])
        for k1, v1 in v['application_details'].items():
            dt.append(
                [v['folder_name'], v1['file_name'], 'Application', v1['modified_time'], v1['size'], v1['file_path']])
        for k1, v1 in v['generator_details'].items():
            dt.append(
                [v['folder_name'], v1['file_name'], 'Generator', v1['modified_time'], v1['size'], v1['file_path']])
        for k1, v1 in v['qvd_details'].items():
            dt.append([v['folder_name'], v1['file_name'], 'QVD', v1['modified_time'], v1['size'], v1['file_path']])
        for k1, v1 in v['user_only_application_details'].items():
            dt.append([v['folder_name'], v1['file_name'], 'User-Only Application', v1['modified_time'], v1['size'],
                       v1['file_path']])
    dframe = pd.DataFrame(dt)
    dframe.columns = columns
    dframe1 = pd.DataFrame(dt1)
    dframe1.columns = columns1
    return dframe1, dframe


def get_meta_details_from_file(path, file_name):
    dct = {'modified_time': datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_name))),
           'size': get_str_size(os.path.getsize(os.path.join(path, file_name))),
           'file_path': os.path.join(path, file_name), 'file_name': file_name}
    return dct


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


def bi7_caller(x):
    return analyze_source_folder(x, QlikViewReportSourcePath, QlikViewReportUserPath)


if __name__ == '__main__':
    start = time.time()
    bi7_source_folders = get_all_folders(QlikViewReportSourcePath)
    print(len(bi7_source_folders), 'folders found in bi7')
    pool = mp.Pool(processes=mp.cpu_count())
    print("Starting to Extract data from folders")
    bi7 = pool.map(bi7_caller, bi7_source_folders)
    pool.close()
    pool.join()
    bi7 = {i['folder_name']: i for i in bi7}
    folder_details, complete_details = df_analysis(bi7)
    writer = pd.ExcelWriter('QVA.xlsx')
    folder_details.to_excel(writer, sheet_name='Overall Bi7 Folder Details', index=False)
    complete_details.to_excel(writer, sheet_name='Bi7 Complete Data', index=False)
    worksheet = writer.sheets['Overall Bi7 Folder Details']
    for i, col in enumerate(folder_details.columns):
        column_len = folder_details[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)
    worksheet = writer.sheets['Bi7 Complete Data']
    for i, col in enumerate(complete_details.columns):
        column_len = complete_details[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)
    writer.save()
    print('Analysis completed! Time taken :', round(time.time() - start, 2), 'seconds')
    with open('QV Analysis.json', 'w') as outfile:
        outfile.write(json.dumps(bi7, indent=4))
    print('Press to continue..')
    input()
