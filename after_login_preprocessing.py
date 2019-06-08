"""
########################### List of Process Tags  ################################
-> [CSV]: CSV format of input log
-> [SF]: Given features are selected and others have been omitted.
-> [Sorted]: Data is sorted by 3 attribute respectively.
-> [ExCol]: Exchanged 2 given columns in csv file.
-> [VDur]: Extracted video durations in video views.
-> [FiNones]: If there is any absent item in csv file, fill with NONEs.
-> [FilteredPV]: Final summarized page views table with added session-durations, link-counts, unique-link-counts.
-> [FilteredVV]: Final summarized video views table with added video-durations, video-counts, unique-video-counts.
-> [NoNulls]: Removed the rows that contain 'NULL' word.
###################################################################################
"""
from preprocess_operations import PreProcessOperations as Ppo


def process_page_views(repo_dir, pv_data_set):
    page_views_processes = ['CSV', 'SF', 'Sorted', 'FilteredPV']
    pv_file_names = Ppo.create_file_names_with_p_tags(repo_dir, pv_data_set, page_views_processes, out_extension='csv')

    Ppo.after_login_log_to_csv(pv_file_names[0])
    Ppo.select_features_in_csv(pv_file_names[1], selected_features=[1, 2, 3, 4, 5])  # ID feature is omitted.
    Ppo.sort_csv_by_header(pv_file_names[2], feature1='user_id', feature2='session_id', feature3='date',
                           feature4='time')
    Ppo.filter_page_views(pv_file_names[3], usr_idx=0, sid_idx=1, date_idx=2, time_idx=3, url_idx=4,
                          time_format='%H:%M:%S.%f', date_format='%Y-%m-%d')

    return pv_file_names[len(page_views_processes)]


def process_video_views(repo_dir, vv_data_set):
    video_views_processes = ['CSV', 'FiNones', 'SF', 'Sorted', 'ExCol', 'VDur', 'FilteredVV']
    vv_file_names = Ppo.create_file_names_with_p_tags(repo_dir, vv_data_set, video_views_processes, out_extension='csv')

    Ppo.after_login_log_to_csv(vv_file_names[0])
    Ppo.fill_with_nones(vv_file_names[1])
    Ppo.select_features_in_csv(vv_file_names[2], selected_features=[1, 2, 3, 4, 5, 6, 7])  # ID feature is omitted.
    Ppo.sort_csv_by_header(vv_file_names[3], feature1='user_id', feature2='session_id', feature3='date',
                           feature4='time')
    Ppo.exchange_columns(vv_file_names[4], feature1='user_id', feature2='session_id')
    Ppo.extract_video_durations(vv_file_names[5], 'date', 'time', 'exit_date', 'exit_time',
                                time_format='%H:%M:%S.%f', date_format='%Y-%m-%d')
    Ppo.filter_video_views(vv_file_names[6], vid_id_idx=0, usr_idx=1, sid_idx=2, vid_dur_idx=5)

    return vv_file_names[len(video_views_processes)]


def process_merged_file(repo_dir, mf_data_set):
    merged_file_processes = ['NoNulls', 'SF']
    mf_file_names = Ppo.create_file_names_with_p_tags(repo_dir, mf_data_set, merged_file_processes, out_extension='csv')

    Ppo.clear_rows_contain_null(mf_file_names[0], null_word='NULL')

    return mf_file_names[len(merged_file_processes) - 1]


def main():
    ds_dir = 'datasets/after_login_datasets/'  # Directory of after-login dataset files.
    video_views_dir = ds_dir + 'video_views_dir/'  # Directory of video-views files and its processed versions.
    page_views_dir = ds_dir + 'page_views_dir/'  # Directory of page-views files and its processed versions.
    merged_file_dir = ds_dir + 'merged_file_dir/'  # Directory of merged files and its processed versions.

    page_views_data_set = 'pageViews.txt'
    video_views_data_set = 'videoViews.txt'
    processed_pv = process_page_views(repo_dir=page_views_dir, pv_data_set=page_views_data_set)
    processed_vv = process_video_views(repo_dir=video_views_dir, vv_data_set=video_views_data_set)

    merged_file_data_set = Ppo.merge_video_page_views(merged_file_dir, processed_pv, processed_vv, uid_idx=0, sid_idx=1)
    processed_mf = process_merged_file(repo_dir=merged_file_dir, mf_data_set=merged_file_data_set)

    print('Final pageViews:%s\nFinal videoViews:%s\nFinal mergedFile:%s' % (processed_pv, processed_vv, processed_mf))
    print('Program has successfully terminated.')


if __name__ == '__main__':
    main()
