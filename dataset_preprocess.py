__csv_delimiter__ = '\",\"'


def log_to_csv(log_file):
    csv_file = log_file.replace('.log', '[CSV].csv')

    input_log_file = open(log_file, encoding='ISO-8859-1')
    output_csv_file = open(csv_file, 'w')

    csv_header = 'date,time,s-ip,cs-method,cs-uri-stem,cs-uri-query,s-port,cs-username,c-ip,cs(User-Agent),' \
                 'cs(Referer),sc-status,sc-substatus,sc-win32-status,time-taken'

    output_csv_file.write(csv_header + '\n')  # Write header to first row of the file.

    count_comment = 0
    count_log = 0
    for line in input_log_file:
        if not line.startswith('#'):  # If row is not comment, consider.
            count_log += 1
            line = line[:-1].replace(' ', __csv_delimiter__)
            line = '\"%s\"\n' % line
            output_csv_file.write(line)
        else:  # If row is comment, don't consider.
            count_comment += 1
    input_log_file.close()
    output_csv_file.close()

    print('INFO: %d out of %d log-rows converted to csv format.' % (count_log, count_log + count_comment))
    print('INFO: %d out of %d comment-rows have been omitted.' % (count_comment, count_log + count_comment))


def clean_bots_from_csv(csv_file, user_agent_column):
    new_csv_file = csv_file.replace('.csv', '[NotBots].csv')
    unprocessed_csv_file = open(csv_file, 'r')
    processed_csv_file = open(new_csv_file, 'w')

    csv_header = unprocessed_csv_file.__next__()
    processed_csv_file.write(csv_header)  # Write header to first row of the file.

    for line in unprocessed_csv_file:
        user_agent = line.split(__csv_delimiter__)[user_agent_column]

        if not user_agent.lower().__contains__('bot'):  # If line doesn't contain 'bot' in any case.
            processed_csv_file.write(line)
        else:
            print("Omitted bot log: " + line[:-1])  # To display the omitted log.
    unprocessed_csv_file.close()
    processed_csv_file.close()


def select_features_in_csv(csv_file, selected_features):
    new_csv_file = csv_file.replace('.csv', '[SF].csv')
    unprocessed_csv_file = open(csv_file, 'r')

    processed_csv_file = open(new_csv_file, 'w')

    csv_header = unprocessed_csv_file.__next__()
    list_header_items = csv_header[:-1].split(',')

    tmp_str = ''
    for column in selected_features:
        tmp_str += list_header_items[column] + ','
    tmp_str = tmp_str[:-1] + '\n'
    processed_csv_file.write(tmp_str)

    for line in unprocessed_csv_file:
        list_header_items = line[1:-2].split(__csv_delimiter__)
        tmp_str = ''
        for column in selected_features:
            tmp_str += list_header_items[column] + __csv_delimiter__

        tmp_str = tmp_str[:-__csv_delimiter__.__len__()] + '\"\n'
        processed_csv_file.write('\"' + tmp_str)
    unprocessed_csv_file.close()
    processed_csv_file.close()


def extract_user_ids(csv_file, ip_column, user_agent_column):
    unprocessed_csv_file = open(csv_file, 'r')
    unprocessed_csv_file.__next__()  # pass csv

    users_set = set() # Set of tuple_of_ip_and_user-agent
    users_dict = {} # key = tuple_of_ip_and_user-agent & value = user_id
    users_dict_count = 0
    for line in unprocessed_csv_file:
        tmp_list = line[+1:-2].split(__csv_delimiter__)
        a_tuple = (tmp_list[ip_column], tmp_list[user_agent_column])
        if not users_set.__contains__(a_tuple):
            users_set.add(a_tuple)
            users_dict[a_tuple] = users_dict_count
            users_dict_count += 1
    unprocessed_csv_file.close()
    return users_dict


def write_user_ids(csv_file, ip_column, user_agent_column, users_dict):
    unprocessed_csv_file = open(csv_file, 'r')
    processed_csv_file = open(csv_file.replace('.csv', '[USERS].csv'), 'w')

    csv_header = unprocessed_csv_file.__next__()
    new_csv_header = csv_header[:-1] + ',' + 'user_id' + '\n'
    processed_csv_file.write(new_csv_header)  # Write new header to first row of the file.

    for line in unprocessed_csv_file:
        tmp_list = line[+1:-2].split(__csv_delimiter__)
        a_tuple = (tmp_list[ip_column], tmp_list[user_agent_column])

        user_id = users_dict.get(a_tuple)
        new_line = line[:-2] + __csv_delimiter__ + str(user_id) + '\"\n'
        processed_csv_file.write(new_line)
    unprocessed_csv_file.close()
    processed_csv_file.close()


def main():
    text_dir_path = 'TextFiles'
    # log_file_name = 'u_extend15.log'
    # log_to_csv('%s/%s' % (text_dir_path, log_file_name))
    # csv_file_name = 'u_extend15[CSV].csv'
    # clean_bots_from_csv('%s/%s' % (text_dir_path, csv_file_name), 9)

    # csv_file_name = 'u_extend15[CSV][NotBots].csv'
    # selected_features = [0, 1, 4, 8, 9, 10]
    # select_features_in_csv('%s/%s' % (text_dir_path, csv_file_name), selected_features)

    csv_file_name = 'u_extend15[CSV][NotBots][SF].csv'
    users_dict = extract_user_ids('%s/%s' % (text_dir_path, csv_file_name), 3, 4)
    write_user_ids('%s/%s' % (text_dir_path, csv_file_name), 3, 4, users_dict)
    exit(0)


if __name__ == '__main__':
    main()
