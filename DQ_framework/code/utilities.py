from imports import *

def read_csv(filepath):
    '''
        Fucntion : Reading the csv File
        Input    : File Path
        Output   : Pandas DataFrame
    '''
    df = pd.read_csv(filepath)
    #df = df.head(n=100000)
    return df

def date_format_validator(val, format):
    try:
        datetime.strptime(val, format)
        return "FORMAT IS VALID"
    except Exception as e:
        return "FORMAT IS NOT VALID"

def regex_validation(val, format):
    if bool(re.match(format, str(val))):
        return "FORMAT IS VALID"
    else:
        return "FORMAT IS NOT VALID"
def read_json(config_file):
    '''
        Fucntion : Reading the JSON File
        Input    : Json File Name
        Output   : Ordered Dictonary
    '''
    parsed_data = None
    if os.path.exists(config_file) and os.path.getsize(config_file) > 0:
        try:
            parsed_data = json.load(open(config_file, 'r'), object_pairs_hook=OrderedDict)
        except Exception as e:
            print('Failed to Read and Load the Json File Please Check the File {}'.format(config_file))
    else:
        raise FileNotFoundError("Config File Not Present in the Given Directory {}".format(config_file))
    return parsed_data


def get_files(patter):
    filesnames = list()
    for name in glob.glob(str(patter)):
        filesnames.append(name)
    return filesnames
