def log_to_csv(log_file):
    csv_file = log_file.replace('.log', ' [CSV].csv')

    input_log_file = open(log_file, encoding='ISO-8859-1')
    output_csv_file = open(csv_file, 'w')

    csv_header = 'date,time,s-ip,cs-method,cs-uri-stem,cs-uri-query,s-port,cs-username,c-ip,cs(User-Agent),' \
                 'cs(Referer),sc-status,sc-substatus,sc-win32-status,time-taken\n'

    output_csv_file.write(csv_header)  # Write header to first row.

    count_comment = 0
    count_log = 0
    for line in input_log_file:
        if not line.startswith('#'):  # If row is not comment, consider.
            count_log += 1
            line = line[:-1].replace(' ', '\",\"')
            line = '\"%s\"\n' % line
            output_csv_file.write(line)
        else:  # If row is comment, don't consider.
            count_comment += 1
    input_log_file.close()
    output_csv_file.close()

    print('INFO: %d out of %d log-rows converted to csv format.' % (count_log, count_log + count_comment))
    print('INFO: %d out of %d comment-rows has been omitted.' % (count_comment, count_log + count_comment))


def main():
    text_dir_path = 'TextFiles'
    log_file_name = 'u_extend15.log'
    log_to_csv('%s/%s' % (text_dir_path, log_file_name))


if __name__ == '__main__':
    main()
