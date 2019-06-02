# ######### PROCESS LIST ############################################################
# CSV: CSV format of input log
# SF: Given features are selected and others have been omitted.
# Sorted: Data is sorted by 3 attribute respectively.
# ExCol: Exchanged 2 given columns in csv file.
# VDur: Extracted video durations in video views.
# FiNones: If there is any absent item in csv file, fill with NONEs.
# FilteredPV: Final summarized page views table with added session-durations, unique-link-counts,link-counts
# ###################################################################################


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
    page_views_processes = ['CSV', 'SF', 'Sorted', 'FilteredPV']
    page_views_data_set = 'pageViews'  # name of data set
    pv_file_names = create_file_names(repo_dir, page_views_data_set, page_views_processes, extension='.csv')

    ppo_pv = preprocess_operations.PreProcessOperations()

    # ppo_pv.preprocessed_log_to_csv(repo_dir + page_views_data_set + '.txt')
    # ppo_pv.select_features_in_csv(pv_file_names[0], selected_features=[1, 2, 3, 4, 5])  # ID feature is omitted.
    # ppo_pv.sort_csv_by_header(pv_file_names[1], feature1='user_id', feature2='session_id', feature3='date',
    # feature4 = 'time')
    # ppo_pv.filter_page_views(pv_file_names[2], usr_idx=0, sid_idx=1, date_idx=2, time_idx=3, url_idx=4,
    # time_format='%H:%M:%S.%f', date_format='%Y-%m-%d')


def process_video_views(repo_dir):
    video_views_processes = ['CSV', 'FiNones', 'SF', 'Sorted', 'ExCol', 'VDur', 'FilteredVV']
    video_views_data_set = 'videoViews'  # name of data set
    vv_file_names = create_file_names(repo_dir, video_views_data_set, video_views_processes, extension='.csv')

    ppo_vv = preprocess_operations.PreProcessOperations()

    ppo_vv.preprocessed_log_to_csv(repo_dir + video_views_data_set + '.txt')
    ppo_vv.fill_with_nones(vv_file_names[0])
    ppo_vv.select_features_in_csv(vv_file_names[1], selected_features=[1, 2, 3, 4, 5, 6, 7])  # ID feature is omitted.
    ppo_vv.sort_csv_by_header(vv_file_names[2], feature1='user_id', feature2='session_id', feature3='date',
                              feature4='time')
    ppo_vv.exchange_columns(vv_file_names[3], feature1='user_id', feature2='session_id')
    ppo_vv.extract_video_durations(vv_file_names[4], 'date', 'time', 'exit_date', 'exit_time',
                                   time_format='%H:%M:%S.%f', date_format='%Y-%m-%d')
    ppo_vv.filter_video_views(vv_file_names[5], vid_id_idx=0, usr_idx=1, sid_idx=2, vid_dur_idx=5)


def main():
    repo_dir = 'Datasets/after_login_datasets/'  # Directory of dataset files
    # process_page_views(repo_dir)
    process_video_views(repo_dir)

    print('Success')


if __name__ == '__main__':
    main()
    exit(0)
