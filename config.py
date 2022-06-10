from configparser import ConfigParser

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get db connection details from section
    
    # dict to hold postgres db connection parameters
    db = {}
    # making sure config file has postgres section
    if parser.has_section(section):
        # parser.items puts items into an array from the ini file
        params = parser.items(section)
        # since the result is an array for each row 0 = 1
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db