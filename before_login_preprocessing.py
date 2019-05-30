import preprocess_operations


def create_file_names(repo_dir, ds_name, process_labels, extension):
    file_names = []
    cum_p_labels = ['[%s]' % process_labels[0]]  # Cumulative Process Labels
    for i in range(1, len(process_labels)):
        cum_p_labels.append('%s[%s]' % (cum_p_labels[i - 1], process_labels[i]))

    for label in cum_p_labels:
        file_names.append(repo_dir + ds_name + label + extension)

    return file_names


def main():
    # ######### PROCESS LIST ######################################
    # CSV: CSV format of input log
    # NoBots: Crawler that visited the page have been removed.
    # SF: Given features are selected and others have been omitted.
    # USERS: User ids are extracted according to their IP and User-Agent info.
    # SORTED: Data is sorted by 3 attribute respectively.
    # SESSION: Session ids are extracted according to delta time between logs that has same User-ID.
    # AllPages: Logs contains image, redirected links and non-spages have been omitted.
    # FilteredTable: Final summarized table have been obtained.
    # #############################################################
    repo_dir = 'Datasets/before_login_datasets/'  # Directory of data set files

    process_labels = ['CSV', 'NoBots', 'SF', 'USERS', 'SORTED', 'AllPages', 'SESSION', 'FilteredTable']
    data_set = 'u_extend15'  # name of data set

    file_names = create_file_names(repo_dir, data_set, process_labels, extension='.csv')
    ppo = preprocess_operations.PreProcessOperations()

    # ppo.raw_log_to_csv(repo_dir + data_set + '.log')
    # ppo.clean_bots_from_csv(file_names[0], user_agent_idx=9)
    # ppo.select_features_in_csv(file_names[1], selected_features=[0, 1, 4, 8, 9, 10])
    # users_dict = ppo.extract_usr_ids(file_names[2], ip_idx=3, usr_agent_idx=4)
    # ppo.write_usr_ids(file_names[2], users_dict, ip_idx=3, usr_agent_idx=4)
    # ppo.sort_csv_by_header(file_names[3], feature1='user_id', feature2='date', feature3='time', feature4=None)
    ppo.clean_non_page_links_from_csv(file_names[4], uid_idx=0, date_idx=1, time_idx=2, url_idx=3)
    ppo.calc_session_ids(file_names[5], user_id_idx=0, date_idx=1, time_idx=2)
    ppo.get_filtered_table(file_names[6], usr_idx=1, sid_idx=0, date_idx=2, time_idx=3, url_idx=4,
                           time_format='%H:%M:%S', date_format='%Y-%m-%d')

    print('Success')


if __name__ == '__main__':
    main()
    exit(0)
