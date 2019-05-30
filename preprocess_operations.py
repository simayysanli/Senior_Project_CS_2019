import pandas as pd

import time_operations as to

__csv_sep__ = '|'  # csv separator


# ################### UTILITY METHODS #####################
def is_nav_link(url):
    web_page_ext = {'00asp', '0aspx', '0ashx', '00htm', '0html', 'shtml',  # web page extensions
                    'xhtml', '00php', '00jsp', '0jspx', '000pl', '00cgi'}

    size_url_extension = 5

    url_splitted = url.rsplit('.', 1)
    if len(url_splitted) == 1:
        return False

    url_ext_part = url_splitted[1]

    if size_url_extension > len(url_ext_part):
        url_extension = '0' * (size_url_extension - len(url_ext_part)) + url_ext_part
    else:
        url_extension = url_ext_part[:size_url_extension]

    if web_page_ext.__contains__(url_extension.lower()):  # does have an extension
        return True
    return False


def list_to_csv_line(a_list):  # Concatenates items of a list by using csv separator
    line = ''
    for item in a_list:
        line += str(item) + __csv_sep__
    return line[:-len(__csv_sep__)]  # Remove last separator character


def line_to_list(a_line):
    return a_line[:-1].split(__csv_sep__)  # omit new line character at the end & split


def link_contains_image(url):
    img_extensions = {'.png', '.jpg', '.ico', '.gif'}

    if img_extensions.__contains__(url[-4:]):
        return True
    return False


# ########################################################

class PreProcessOperations:
    @staticmethod
    def raw_log_to_csv(log_file):
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

        print('> INFO: %d out of %d log-rows converted to csv format.' % (count_log, count_log + count_comment))
        print('> INFO: %d out of %d comment-rows have been omitted.' % (count_comment, count_log + count_comment))
        print('> Function has successfully terminated and new file named \'%s\' created.' % csv_file)
        print('-' * 80)

    @staticmethod
    def preprocessed_log_to_csv(log_file):
        if '.' in log_file:
            csv_file = log_file.split('.')[0] + '[CSV].csv'
        else:
            csv_file = log_file + '[CSV].csv'

        input_log_file = open(log_file)
        output_csv_file = open(csv_file, 'w')

        csv_header_replacements_tuples = [('kullaniciid', 'user_id'), ('loginid', 'session_id'),
                                          ('cikistarih', 'exit_date exit_time'), ('videoid', 'video_id'),
                                          ('tarih', 'date time')]

        csv_header_line = ' '.join(input_log_file.__next__().split()).lower()
        for old_header_item, new_header_item in csv_header_replacements_tuples:
            csv_header_line = csv_header_line.replace(old_header_item, new_header_item)

        csv_header = csv_header_line.replace(' ', __csv_sep__)
        output_csv_file.write(csv_header)  # Write header to first row of the file.

        with open(log_file) as f:
            num_of_rows = sum(1 for _ in f)

        input_log_file.__next__()  # Pass line filled with dashes
        for i in range(num_of_rows - 5):  # Reduce first 2 rows and last 3 rows
            line = input_log_file.__next__()
            spaces_reduced_row = ' '.join(line.split())
            spaces_reduced_row = spaces_reduced_row.replace(' ', __csv_sep__)
            output_csv_file.write(spaces_reduced_row + '\n')
        input_log_file.close()
        output_csv_file.close()

        print('> Function has successfully terminated and new file named \'%s\' created.' % csv_file)
        print('-' * 80)

    @staticmethod
    def clean_bots_from_csv(csv_file, user_agent_idx):
        bot_log_count = 0
        new_csv_file = csv_file.replace('.csv', '[NoBots].csv')
        unprocessed_csv_file = open(csv_file, 'r')
        processed_csv_file = open(new_csv_file, 'w')

        csv_header = unprocessed_csv_file.__next__()  # pass csv header
        processed_csv_file.write(csv_header)  # Write same header to first row of the file.

        for line in unprocessed_csv_file:
            user_agent = line_to_list(line)[user_agent_idx]

            if not user_agent.lower().__contains__('bot'):  # If line doesn't contain 'bot' in any case.
                processed_csv_file.write(line)
            else:
                bot_log_count += 1
        unprocessed_csv_file.close()
        processed_csv_file.close()

        print('> INFO: %d bot logs have been omitted.' % bot_log_count)
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
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
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
    def extract_usr_ids(csv_file, ip_idx, usr_agent_idx):
        unprocessed_csv_file = open(csv_file, 'r')
        unprocessed_csv_file.__next__()  # pass csv header

        users_set = set()  # Set of tuple_of_ip_and_user-agent
        users_dict = {}  # key = tuple_of_ip_and_user-agent & value = user_id
        users_dict_count = 0
        for line in unprocessed_csv_file:
            tmp_list = line_to_list(line)
            a_tuple = (tmp_list[ip_idx], tmp_list[usr_agent_idx])
            if not users_set.__contains__(a_tuple):
                users_set.add(a_tuple)
                users_dict[a_tuple] = users_dict_count
                users_dict_count += 1
        unprocessed_csv_file.close()
        return users_dict

    @staticmethod
    def write_usr_ids(csv_file, users_dict, ip_idx, usr_agent_idx):
        unprocessed_csv_file = open(csv_file, 'r')
        new_csv_file = csv_file.replace('.csv', '[USERS].csv')
        processed_csv_file = open(new_csv_file, 'w')

        csv_header = unprocessed_csv_file.__next__()
        new_csv_header = 'user_id' + __csv_sep__ + csv_header
        processed_csv_file.write(new_csv_header)  # Write new header to first row of the file.

        for line in unprocessed_csv_file:
            tmp_list = line_to_list(line)
            a_tuple = (tmp_list[ip_idx], tmp_list[usr_agent_idx])

            user_id = users_dict.get(a_tuple)
            new_line = str(user_id) + __csv_sep__ + line
            processed_csv_file.write(new_line)
        unprocessed_csv_file.close()
        processed_csv_file.close()
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
    def sort_csv_by_header(csv_file, feature1, feature2, feature3, feature4):  # sort up to 4 header
        df = pd.read_csv(csv_file, sep=__csv_sep__)

        if feature2 is None:  # Sort by 1 header
            df = df.sort_values([feature1])
        elif feature3 is None:  # Sort by 2 headers
            df = df.sort_values([feature1, feature2])
        elif feature4 is None:  # Sort by 3 headers
            df = df.sort_values([feature1, feature2, feature3])
        else:  # Sort by 4 headers
            df = df.sort_values([feature1, feature2, feature3, feature4])

        new_csv_file = csv_file.replace('.csv', '[SORTED].csv')
        df.to_csv(new_csv_file, index=False, sep=__csv_sep__)
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
    def calc_session_ids(csv_file, user_id_idx, date_idx, time_idx):
        unprocessed_csv_file = open(csv_file, 'r')
        new_csv_file = csv_file.replace('.csv', '[SESSION].csv')
        processed_csv_file = open(new_csv_file, 'w')

        csv_header = unprocessed_csv_file.__next__()

        new_header = 'session_id' + __csv_sep__ + csv_header
        processed_csv_file.write(new_header)  # Write new header to new file.

        first_ln = unprocessed_csv_file.__next__()
        processed_csv_file.write('0' + __csv_sep__ + first_ln)  # write first row with session_id = 0

        t_o = to.TimeOperations(time_format='%H:%M:%S', date_format='%Y-%m-%d')

        threshold_sec = 600  # 10 minutes

        prev_ln = line_to_list(first_ln)
        session_id = 0  # initialize session id
        for line in unprocessed_csv_file:
            cur_ln = line_to_list(line)

            if cur_ln[user_id_idx] != prev_ln[user_id_idx]:  # different user
                session_id += 1
            else:  # same user
                real_diff = t_o.calc_time_diff(cur_ln, prev_ln, date_idx, time_idx)
                if real_diff > threshold_sec:  # same user && large diff
                    session_id += 1
            new_line = str(session_id) + __csv_sep__ + line

            processed_csv_file.write(new_line)

            prev_ln = cur_ln
        unprocessed_csv_file.close()
        processed_csv_file.close()
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
    def clean_non_page_links_from_csv(csv_file, uid_idx, date_idx, time_idx, url_idx):  # Will be added to paper
        new_csv_file = csv_file.replace('.csv', '[AllPages].csv')
        input_csv = open(csv_file, 'r')
        output_csv = open(new_csv_file, 'w')

        csv_header = input_csv.__next__()  # pass csv header
        output_csv.write(csv_header)  # Write same header to first row of the file.
        prev_ln = line_to_list(input_csv.__next__())  # first line

        cnt_non_pages_log = 0
        cnt_all_log = 1

        for line in input_csv:
            cnt_all_log += 1
            cur_ln = line_to_list(line)
            sid_time_tup0 = (prev_ln[uid_idx], prev_ln[date_idx], prev_ln[time_idx])
            sid_time_tup1 = (cur_ln[uid_idx], prev_ln[date_idx], cur_ln[time_idx])

            both_nav_links = is_nav_link(prev_ln[url_idx]) and is_nav_link(cur_ln[url_idx])
            prev_ln_is_redirected_link = (sid_time_tup0 == sid_time_tup1 and both_nav_links)
            if link_contains_image(prev_ln[url_idx]) or not is_nav_link(prev_ln[url_idx]) or prev_ln_is_redirected_link:
                cnt_non_pages_log += 1  # omitted 1 log.
            else:
                output_csv.write(list_to_csv_line(prev_ln) + '\n')

            prev_ln = cur_ln

        if not link_contains_image(prev_ln[url_idx]):
            output_csv.write(list_to_csv_line(prev_ln) + '\n')  # handle last line

        input_csv.close()
        output_csv.close()
        print('> INFO: %d out of %d rows that contains non-pages have been omitted.' % (cnt_non_pages_log, cnt_all_log))
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)

    @staticmethod
    def get_filtered_table(csv_file, usr_idx, sid_idx, date_idx, time_idx, url_idx, time_format, date_format):
        #
        # Will be added to paper
        #
        session_avg_wait_t = 20  # estimated average waiting time (in secs) in a single link.

        new_csv_file = csv_file.replace('.csv', '[FilteredTable].csv')
        input_csv = open(csv_file, 'r')
        output_csv = open(new_csv_file, 'w')
        t_o = to.TimeOperations(time_format=time_format, date_format=date_format)

        new_csv_header_items = ['user_id', 'session_id', 'link_count', 'unique_link_count', 'session_duration_in_s']

        new_csv_header = list_to_csv_line(new_csv_header_items)
        output_csv.write(new_csv_header + '\n')  # Write new header to first row of the file.

        input_csv.__next__()  # Pass CSV Header.
        prev_ln = session_1st_ln = line_to_list(input_csv.__next__())
        link_cnt = unq_link_cnt = 0
        unique_links_set = set()

        for line in input_csv:
            cur_ln = line_to_list(line)

            link_cnt += 1
            if not unique_links_set.__contains__(prev_ln[url_idx]):
                unique_links_set.add(prev_ln[url_idx])
                unq_link_cnt += 1

            if cur_ln[sid_idx] != prev_ln[sid_idx]:  # different session
                session_duration = session_avg_wait_t + t_o.calc_time_diff(prev_ln, session_1st_ln, date_idx, time_idx)
                csv_list = [prev_ln[usr_idx], prev_ln[sid_idx], link_cnt, unq_link_cnt, session_duration]
                output_csv.write(list_to_csv_line(csv_list) + '\n')

                unq_link_cnt = link_cnt = 0
                unique_links_set = set()
                session_1st_ln = cur_ln

            prev_ln = cur_ln

        # handle last line
        last_ln = prev_ln
        link_cnt += 1
        if not unique_links_set.__contains__(last_ln[url_idx]):
            unq_link_cnt += 1
        duration = session_avg_wait_t + t_o.calc_time_diff(prev_ln, session_1st_ln, date_idx, time_idx)
        csv_list = [last_ln[sid_idx], last_ln[usr_idx], link_cnt, unq_link_cnt, duration, 'n/a', 'n/a', 'n/a']
        output_csv.write(list_to_csv_line(csv_list) + '\n')
        input_csv.close()
        output_csv.close()
        print('> Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)
        print('-' * 80)
