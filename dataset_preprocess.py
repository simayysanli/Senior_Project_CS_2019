import pandas as pd
from datetime import datetime

__csv_sep__ = '|'  # csv separator
__text_dir__ = 'TextFiles/'  # Directory of text files
__time_format__ = '%H:%M:%S'  # 12:26:52
__date_format__ = '%Y-%m-%d'  # 2018-11-05
__threshold_secs__ = 10 * 60  # 10 minutes


def list_to_csv_line(a_list):  # Concatenates items of a list by using csv separator
    line = ''
    for item in a_list:
        line += item + __csv_sep__
    return line[:-len(__csv_sep__)]  # Remove last separator character


def line_to_list(a_line):
    return a_line[:-1].split(__csv_sep__)


def log_to_csv(log_file):
    csv_file = log_file.replace('.log', '[CSV].csv')

    input_log_file = open(log_file, encoding='ISO-8859-1')
    output_csv_file = open(csv_file, 'w')

    csv_header_items = ['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query',
                        's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Referer)',
                        'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken']

    csv_header = list_to_csv_line(csv_header_items)
    output_csv_file.write(csv_header + '\n')  # Write header to first row of the file.

    count_comment = 0
    count_log = 0
    for line in input_log_file:
        if not line.startswith('#'):  # If row is not comment, consider.
            count_log += 1
            line = line[:-1].replace(' ', __csv_sep__)
            output_csv_file.write(line + '\n')
        else:  # If row is comment, don't consider.
            count_comment += 1
    input_log_file.close()
    output_csv_file.close()

    print('INFO: %d out of %d log-rows converted to csv format.' % (count_log, count_log + count_comment))
    print('INFO: %d out of %d comment-rows have been omitted.' % (count_comment, count_log + count_comment))
    print('Function has successfully terminated and new file named \'%s\' created.' % csv_file)


def clean_bots_from_csv(csv_file, user_agent_col):
    count_botlog = 0
    new_csv_file = csv_file.replace('.csv', '[NoBots].csv')
    unprocessed_csv_file = open(csv_file, 'r')
    processed_csv_file = open(new_csv_file, 'w')

    for line in unprocessed_csv_file:
        user_agent = line_to_list(line)[user_agent_col]

        if not user_agent.lower().__contains__('bot'):  # If line doesn't contain 'bot' in any case.
            processed_csv_file.write(line)
        else:
            count_botlog += 1
    unprocessed_csv_file.close()
    processed_csv_file.close()

    print('INFO: %d bot logs have been omitted.' % count_botlog)
    print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)


def select_features_in_csv(csv_file, selected_features):
    new_csv_file = csv_file.replace('.csv', '[SF].csv')

    unprocessed_csv_file = open(csv_file, 'r')
    processed_csv_file = open(new_csv_file, 'w')

    for line in unprocessed_csv_file:
        item_list = line_to_list(line)

        new_item_list = []
        for col in selected_features:
            new_item_list.append(item_list[col])

        processed_csv_file.write(list_to_csv_line(new_item_list) + '\n')
    unprocessed_csv_file.close()
    processed_csv_file.close()
    print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)


def extract_user_ids(csv_file, ip_col, user_agent_col):
    unprocessed_csv_file = open(csv_file, 'r')
    unprocessed_csv_file.__next__()  # pass csv

    users_set = set()  # Set of tuple_of_ip_and_user-agent
    users_dict = {}  # key = tuple_of_ip_and_user-agent & value = user_id
    users_dict_count = 0
    for line in unprocessed_csv_file:
        tmp_list = line_to_list(line)
        a_tuple = (tmp_list[ip_col], tmp_list[user_agent_col])
        if not users_set.__contains__(a_tuple):
            users_set.add(a_tuple)
            users_dict[a_tuple] = users_dict_count
            users_dict_count += 1
    unprocessed_csv_file.close()
    return users_dict


def write_user_ids(csv_file, ip_col, user_agent_col, users_dict):
    unprocessed_csv_file = open(csv_file, 'r')
    new_csv_file = csv_file.replace('.csv', '[USERS].csv')
    processed_csv_file = open(new_csv_file, 'w')

    csv_header = unprocessed_csv_file.__next__()
    new_csv_header = 'user_id' + __csv_sep__ + csv_header
    processed_csv_file.write(new_csv_header)  # Write new header to first row of the file.

    for line in unprocessed_csv_file:
        tmp_list = line_to_list(line)
        a_tuple = (tmp_list[ip_col], tmp_list[user_agent_col])

        user_id = users_dict.get(a_tuple)
        new_line = str(user_id) + __csv_sep__ + line
        processed_csv_file.write(new_line)
    unprocessed_csv_file.close()
    processed_csv_file.close()
    print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)


def sort_csv_by_header(csv_file, header_item1, header_item2, header_item3):
    df = pd.read_csv(csv_file, sep=__csv_sep__)
    df = df.sort_values([header_item1, header_item2, header_item3])

    new_csv_file = csv_file.replace('.csv', '[SORTED].csv')
    df.to_csv(new_csv_file, index=False, sep=__csv_sep__)
    print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)


def str_to_time(str_time):
    return datetime.strptime(str_time, __time_format__)


def str_to_date(str_date):
    return datetime.strptime(str_date, __date_format__)


def is_diff_gt_threshold(d2, t2, d1, t1, threshold_secs=__threshold_secs__):
    secs_diff = abs(str_to_time(t2) - str_to_time(t1)).seconds

    if d1 == d2:  # Same day
        # print('DIFFERENCE:' + str(secs_diff) + 's')
        return secs_diff > threshold_secs

    days_diff = (str_to_date(d2) - str_to_date(d1)).days

    if days_diff > 1:
        # print('DIFFERENCE: TOO LARGE!')
        return True
    elif days_diff == 1:
        secs_in_a_day = 86400
        secs_diff = secs_in_a_day - secs_diff
        # print('DIFFERENCE:' + str(secs_diff) + 's')
        return secs_diff > threshold_secs
    else:  # days_diff < 0
        print('ERROR: Wrong date infos for is_diff_bigger_than_threshold function.')
        exit(-1)

def calc_session_ids(csv_file, user_id_col, date_col, time_col):
    unprocessed_csv_file = open(csv_file, 'r')
    new_csv_file = csv_file.replace('.csv', '[SESSION].csv')
    processed_csv_file = open(new_csv_file, 'w')

    csv_header = unprocessed_csv_file.__next__()
    new_csv_header = 'session_id' + __csv_sep__ + csv_header
    processed_csv_file.write(new_csv_header)  # Write new header to first row of the file.

    old_user_id = 0
    session_id = 0

    first_line = unprocessed_csv_file.__next__()
    old_date = line_to_list(first_line)[date_col]
    old_time = line_to_list(first_line)[time_col]
    processed_csv_file.write('0' + __csv_sep__ + first_line)

    for line in unprocessed_csv_file:
        list_items = line_to_list(line)
        cur_user_id = int(list_items[user_id_col])

        cur_date = list_items[date_col]
        cur_time = list_items[time_col]

        if cur_user_id != old_user_id or \
                is_diff_gt_threshold(cur_date, cur_time, old_date, old_time):
            session_id += 1
        new_line = str(session_id) + __csv_sep__ + line
        processed_csv_file.write(new_line)

        old_time = cur_time
        old_date = cur_date
        old_user_id = cur_user_id


def main():
    ##########################################################################################
    # log_file_name = 'u_extend15.log'
    # log_to_csv(__text_dir__ + log_file_name)
    ##########################################################################################
    # csv_file_name = 'u_extend15[CSV].csv'
    # clean_bots_from_csv(__text_dir__ + csv_file_name, user_agent_col=9)
    ##########################################################################################
    # csv_file_name = 'u_extend15[CSV][NoBots].csv'
    # selected_features = [0, 1, 4, 8, 9, 10]
    # select_features_in_csv(__text_dir__ + csv_file_name, selected_features)
    ##########################################################################################
    # csv_file_name = 'u_extend15[CSV][NoBots][SF].csv'
    # users_dict = extract_user_ids(__text_dir__ + csv_file_name, 3, 4)
    # write_user_ids(__text_dir__ + csv_file_name, 3, 4, users_dict)
    ##########################################################################################
    # csv_file_name = 'u_extend15[CSV][NoBots][SF][USERS].csv'
    # sort_csv_by_header(__text_dir__ + csv_file_name, 'user_id', 'date', 'time')
    ##########################################################################################
    csv_file_name = 'u_extend15[CSV][NoBots][SF][USERS][SORTED].csv'
    calc_session_ids(__text_dir__ + csv_file_name, user_id_col=0, date_col=1, time_col=2)
    exit(0)


if __name__ == '__main__':
    main()
