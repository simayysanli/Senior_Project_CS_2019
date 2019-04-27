import pandas as pd
from datetime import datetime

__csv_sep__ = '|'  # csv separator


class TimeOperations:
    def __init__(self, time_format, date_format):
        self.time_format = time_format
        self.date_format = date_format

    def __str_to_time(self, str_time):
        return datetime.strptime(str_time, self.time_format)

    def __str_to_date(self, str_date):
        return datetime.strptime(str_date, self.date_format)

    def calc_time_diff(self, list2, list1, date_idx, time_idx):
        d2 = list2[date_idx]
        t2 = list2[time_idx]
        d1 = list1[date_idx]
        t1 = list1[time_idx]

        secs_in_a_day = 86400
        secs_diff = abs(self.__str_to_time(t2) - self.__str_to_time(t1)).seconds

        if d1 == d2:  # Same day
            return secs_diff

        days_diff = (self.__str_to_date(d2) - self.__str_to_date(d1)).days
        if days_diff > 1:  # DIFFERENCE = more than 1 day!
            return secs_in_a_day + 1
        elif days_diff == 1:
            return secs_in_a_day - secs_diff
        else:  # days difference is negative
            print('Error in input data, Dates are not ordered')
            print('days_dif: ' + str(days_diff))
            exit(-1)


class PreProcessOperations:

    @staticmethod
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

    @staticmethod
    def clean_bots_from_csv(csv_file, user_agent_idx):
        count_botlog = 0
        new_csv_file = csv_file.replace('.csv', '[NoBots].csv')
        unprocessed_csv_file = open(csv_file, 'r')
        processed_csv_file = open(new_csv_file, 'w')

        unprocessed_csv_file.__next__()  # pass csv header

        for line in unprocessed_csv_file:
            user_agent = line_to_list(line)[user_agent_idx]

            if not user_agent.lower().__contains__('bot'):  # If line doesn't contain 'bot' in any case.
                processed_csv_file.write(line)
            else:
                count_botlog += 1
        unprocessed_csv_file.close()
        processed_csv_file.close()

        print('INFO: %d bot logs have been omitted.' % count_botlog)
        print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)

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
        print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)

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
        print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)

    @staticmethod
    def sort_csv_by_header(csv_file, header_item1, header_item2, header_item3):
        df = pd.read_csv(csv_file, sep=__csv_sep__)
        df = df.sort_values([header_item1, header_item2, header_item3])

        new_csv_file = csv_file.replace('.csv', '[SORTED].csv')
        df.to_csv(new_csv_file, index=False, sep=__csv_sep__)
        print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)

    @staticmethod
    def calc_session_ids(csv_file, user_id_idx, date_idx, time_idx):
        unprocessed_csv_file = open(csv_file, 'r')
        new_csv_file = csv_file.replace('.csv', '[SESSION].csv')
        processed_csv_file = open(new_csv_file, 'w')
        csv_sep = __csv_sep__

        csv_header = unprocessed_csv_file.__next__()
        new_header = 'session_id' + csv_sep + csv_header
        processed_csv_file.write(new_header)  # Write new header to new file.

        first_ln = unprocessed_csv_file.__next__()
        processed_csv_file.write('0' + csv_sep + first_ln)  # write first row with session_id = 0

        t_o = TimeOperations(time_format='%H:%M:%S', date_format='%Y-%m-%d')

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

            new_line = str(session_id) + '|' + line

            processed_csv_file.write(new_line)

            prev_ln = cur_ln

    @staticmethod
    def clean_img_from_csv(csv_file, sid_idx, uri_stem_idx):
        count_img_log = 0
        count_all_log = 0
        img_extensions = {'.png', '.jpg', '.ico', '.gif'}
        new_csv_file = csv_file.replace('.csv', '[NoImgs].csv')
        unprocessed_csv_file = open(csv_file, 'r')
        processed_csv_file = open(new_csv_file, 'w')

        csv_header = unprocessed_csv_file.__next__()  # pass csv header
        processed_csv_file.write(csv_header)  # Write same header to first row of the file.

        prev_session = -1

        for line in unprocessed_csv_file:
            count_all_log += 1
            listed_line = line_to_list(line)
            user_agent = listed_line[uri_stem_idx]
            cur_session = int(listed_line[sid_idx])

            if prev_session == cur_session and img_extensions.__contains__(user_agent[-4:]):
                count_img_log += 1
            else:
                processed_csv_file.write(line)
            prev_session = cur_session

        unprocessed_csv_file.close()
        processed_csv_file.close()

        print('INFO: %d out of %d image-containing-rows have been omitted.' % (count_img_log, count_all_log))
        print('Function has successfully terminated and new file named \'%s\' created.' % new_csv_file)

    @staticmethod
    def get_filtered_table(csv_file, sid_idx, usr_idx, date_idx, time_idx, uri_stem_idx):
        session_avg_wait_t = 20  # estimated average waiting time in a single link.

        new_csv_file = csv_file.replace('.csv', '[FilteredTable].csv')
        input_csv = open(csv_file, 'r')
        output_csv = open(new_csv_file, 'w')
        t_o = TimeOperations(time_format='%H:%M:%S', date_format='%Y-%m-%d')

        new_csv_header_items = ['session_id', 'user_id', 'link_count', 'unique_link_count', 'duration', 'pdf_click',
                                'video_click', 'test_solved']

        new_csv_header = list_to_csv_line(new_csv_header_items)
        output_csv.write(new_csv_header + '\n')  # Write new header to first row of the file.

        input_csv.__next__()  # Pass CSV Header.
        prev_ln = session_1st_ln = line_to_list(input_csv.__next__())
        link_cnt = unq_link_cnt = 1
        unique_links_set = set()

        for line in input_csv:
            cur_ln = line_to_list(line)
            if is_navigation_link(prev_ln[uri_stem_idx]):
                link_cnt += 1
                if not unique_links_set.__contains__(prev_ln[uri_stem_idx]):
                    unique_links_set.add(prev_ln[uri_stem_idx])
                    unq_link_cnt += 1

            if cur_ln[sid_idx] != prev_ln[sid_idx]:  # different session
                duration = session_avg_wait_t + t_o.calc_time_diff(prev_ln, session_1st_ln, date_idx, time_idx)
                csv_list = [prev_ln[sid_idx], prev_ln[usr_idx], link_cnt, unq_link_cnt, duration, 'n/a', 'n/a', 'n/a']
                output_csv.write(list_to_csv_line(csv_list) + '\n')

                unq_link_cnt = link_cnt = 1
                unique_links_set = set()
                session_1st_ln = cur_ln

            prev_ln = cur_ln

        # handle last line
        last_ln = prev_ln
        if is_navigation_link(last_ln[uri_stem_idx]):
            link_cnt += 1
            if not unique_links_set.__contains__(last_ln[uri_stem_idx]):
                unq_link_cnt += 1
        duration = session_avg_wait_t + t_o.calc_time_diff(prev_ln, session_1st_ln, date_idx, time_idx)
        csv_list = [last_ln[sid_idx], last_ln[usr_idx], link_cnt, unq_link_cnt, duration, 'n/a', 'n/a', 'n/a']
        output_csv.write(list_to_csv_line(csv_list) + '\n')


def list_to_csv_line(a_list):  # Concatenates items of a list by using csv separator
    line = ''
    for item in a_list:
        line += str(item) + __csv_sep__
    return line[:-len(__csv_sep__)]  # Remove last separator character


def line_to_list(a_line):
    return a_line[:-1].split(__csv_sep__)


def create_file_names(repo_dir, ds_name, process_labels, extension):
    file_names = []
    cum_p_labels = ['[%s]' % process_labels[0]]  # Cumulative Process Labels
    for i in range(1, len(process_labels)):
        cum_p_labels.append('%s[%s]' % (cum_p_labels[i - 1], process_labels[i]))

    for label in cum_p_labels:
        file_names.append(repo_dir + ds_name + label + extension)

    return file_names


def is_navigation_link(uri_stem):
    web_page_ext = {'asp', 'aspx', 'htm', 'html', 'shtml',  # web page extensions
                    'xhtml', 'php', 'jsp', 'jspx', 'pl', 'cgi'}

    uri_stem_splited = uri_stem.rsplit('.', 1)

    if len(uri_stem_splited) == 1 or web_page_ext.__contains__(uri_stem_splited[1]):  # does have an extension
        return True
    return False


def main():
    repo_dir = 'TextFiles/'  # Directory of data set files
    data_set = 'u_extend15'  # name of data set
    process_labels = ['CSV', 'NoBots', 'SF', 'USERS', 'SORTED', 'SESSION', 'NoImgs', 'FilteredTable']
    file_names = create_file_names(repo_dir, data_set, process_labels, extension='.csv')
    ppo = PreProcessOperations()

    ppo.log_to_csv(repo_dir + data_set + '.log')
    ppo.clean_bots_from_csv(file_names[0], user_agent_idx=9)
    ppo.select_features_in_csv(file_names[1], selected_features=[0, 1, 4, 8, 9, 10])
    users_dict = ppo.extract_usr_ids(file_names[2], ip_idx=3, usr_agent_idx=4)
    ppo.write_usr_ids(file_names[2], users_dict, ip_idx=3, usr_agent_idx=4)
    ppo.sort_csv_by_header(file_names[3], header_item1='user_id', header_item2='date', header_item3='time')
    ppo.calc_session_ids(file_names[4], user_id_idx=0, date_idx=1, time_idx=2)
    ppo.clean_img_from_csv(file_names[5], sid_idx=0, uri_stem_idx=4)

    # clean forward link?
    ppo.get_filtered_table(file_names[6], sid_idx=0, usr_idx=1, date_idx=2, time_idx=3, uri_stem_idx=4)

    print('Success')


if __name__ == '__main__':
    main()
    exit(0)
