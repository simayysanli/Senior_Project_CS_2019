# ######### PROCESS LIST ######################################
# CSV: CSV format of input log
# SF: Given features are selected and others have been omitted.
# SORTED: Data is sorted by 3 attribute respectively.
# FilteredTable: Final summarized table have been obtained.
# #############################################################


import preprocess_operations


def create_file_names(repo_dir, ds_name, process_labels, extension):
    file_names = []
    cum_p_labels = ['[%s]' % process_labels[0]]  # Cumulative Process Labels
    for i in range(1, len(process_labels)):
        cum_p_labels.append('%s[%s]' % (cum_p_labels[i - 1], process_labels[i]))

    for label in cum_p_labels:
        file_names.append(repo_dir + ds_name + label + extension)

    return file_names


def process_page_views(repo_dir):
    page_views_processes = ['CSV', 'SF', 'SORTED', 'FilteredTable']
    page_views_data_set = 'pageViews'  # name of data set
    pv_file_names = create_file_names(repo_dir, page_views_data_set, page_views_processes, extension='.csv')

    ppo_pv = preprocess_operations.PreProcessOperations()

    ppo_pv.preprocessed_log_to_csv(repo_dir + page_views_data_set + '.txt')
    exit(0)
    ppo_pv.select_features_in_csv(pv_file_names[0], selected_features=[1, 2, 3, 4, 5])  # ID feature is omitted.
    ppo_pv.sort_csv_by_header(pv_file_names[1], feature1='user_id', feature2='session_id', feature3='date',
                              feature4='time')
    ppo_pv.get_filtered_table(pv_file_names[2], usr_idx=0, sid_idx=1, date_idx=2, time_idx=3, url_idx=4,
                              time_format='%H:%M:%S.%f', date_format='%Y-%m-%d')


def process_video_views(repo_dir):
    video_views_processes = ['CSV', 'SF', 'SORTED']
    video_views_data_set = 'videoViews'  # name of data set
    vv_file_names = create_file_names(repo_dir, video_views_data_set, video_views_processes, extension='.csv')

    ppo_vv = preprocess_operations.PreProcessOperations()
    ppo_vv.preprocessed_log_to_csv(repo_dir + video_views_data_set + '.txt')
    ppo_vv.select_features_in_csv(vv_file_names[0], selected_features=[1, 2, 3, 4, 5])  # ID feature is omitted.
    ppo_vv.sort_csv_by_header(vv_file_names[1], feature1='user_id', feature2='session_id', feature3='date',
                              feature4='time')


def main():
    repo_dir = 'Datasets/after_login_datasets/'
    process_page_views(repo_dir)
    process_video_views(repo_dir)

    print('Success')


if __name__ == '__main__':
    main()
    exit(0)
