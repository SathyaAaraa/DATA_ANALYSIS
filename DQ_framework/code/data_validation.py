#Import required packages
from imports import *
from utilities import *

#Initialise required directory
parent_dir = dirname(dirname(abspath(__file__)))
config_dir = os.path.join(parent_dir, "config")
input_dir = os.path.join(parent_dir, "input")
output_dir = os.path.join(parent_dir, "output")


class DataQualityV1():
    def __init__(self):
        '''
        Initialise and read all configuration parameters
        '''
        config = ConfigParser()
        config.read(config_dir + '\data_quality.ini')
        self.config_filename = config.get('job-parameter', 'config_file_name')
        self.threshold_check_on_DQ = config.get('job-parameter', 'threshold_check_on_DQ')
        self.threshold_percentage = config.get('job-parameter', 'threshold_percentage')

    def data_quality(self):
        '''
        data_quality method validates whether columns are matching with configured format.
        This method outputs 2 files if threshold_check_on_DQ is set to Y - DQ_SUMMARY and DQ_DETAILED_INFO.
        This method outputs 1 file if threshold_check_on_DQ is set to N - DQ_DETAILED_INFO.
        :return:
        '''
        #Read config.json file
        config_data = read_json(config_file= config_dir + "\\" + self.config_filename)
        #Read files with configured pattern in input directory
        get_fileNames = get_files(input_dir + "\\" + config_data['FILENAME_PATTERN'])

        #Iterate file present in input directory
        for each_file in get_fileNames:
            #split file path to get file name
            each_filename = each_file.split('\\')[-1]
            dq_column_list = []
            #if file extension is csv call read_csv to read file and store in pandas df
            if config_data['FILE_TYPE'] == 'CSV':
                df = read_csv(each_file)
                df_length = len(df)

            #Iterate config data
            #Verify whether data matches with configured format for configured columns
            for columnname, description in config_data['COLUMNVALIDATOR'].items():

                if description['TYPE'] == 'DATETIME':
                    df['DQ_CHECK_ON'+columnname] = df[columnname].apply(lambda val : date_format_validator(val, description['FORMAT']))
                    dq_column_list.append('DQ_CHECK_ON'+columnname)

                elif description['TYPE'] == 'VARCHAR':
                    df['DQ_CHECK_ON' + columnname] = df[columnname].apply(
                        lambda val: regex_validation(val, description['FORMAT']))
                    dq_column_list.append('DQ_CHECK_ON' + columnname)
                else:
                    pass

            df.to_csv(output_dir + '\\DQ_DETAILED_INFO_' + each_filename, index=False)

            #If threshold_check_on_DQ is set to Y, get valid, invalid record count.'
            #If invalid record count is greater than or equal to threshold limit then DQ is FALSE (i.e) DQ has failed
            #If invalid record count is less than threshold limit then DQ is TRUE (i.e) DQ has Passed
            if self.threshold_check_on_DQ == 'Y':
                print("Threshold check is set to Y")
                number_of_bad_records_limit = int((int(self.threshold_percentage)/100) * df_length)

                threshold_check_df_list = []
                for each_col in dq_column_list:
                    valid_count = len(df[df[each_col]=='FORMAT IS VALID'])
                    invalid_count = len(df[df[each_col]=='FORMAT IS NOT VALID'])
                    threshold_check_df_list.append([each_col, df_length, valid_count, invalid_count, number_of_bad_records_limit, int(invalid_count)<int(number_of_bad_records_limit)])
                threshold_check_df = pd.DataFrame(threshold_check_df_list, columns= ['column_NAME', 'total records', 'valid_record_count', 'invalid_record_count', 'Allowed_bad_record_limit', 'DQ-True/FALSE'])

                threshold_check_df.to_csv(output_dir + '\\DQ_SUMMARY_' + each_filename, index=False)
                print("Please find DQ Summary in path " + output_dir + '\\DQ_SUMMARY_' + each_filename)
                print("Please find detailed information in path " + output_dir + '\\DQ_DETAILED_INFO_' + each_filename)
            else:
                print("Please find detailed information in path " + output_dir + '\\DQ_DETAILED_INFO_' + each_filename)

if __name__ == '__main__':

    #Log Start Timestamp
    StartTimeStamp = datetime.now()

    data_quality_obj = DataQualityV1()
    data_quality_obj.data_quality()

    #Log End Timestamp
    EndTimeStamp = datetime.now()

    print("Time taken in seconds : ", str(round((EndTimeStamp - StartTimeStamp).total_seconds(), 2)))