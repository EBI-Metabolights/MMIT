import sys
import config
import configparser
import logging
import getopt
import os
import json
from aws_client import aws_download_file

msg_format = '%(asctime)s %(levelname)s %(message)s'
date_format= '%Y-%d-%m %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=date_format, level=logging.INFO)
logger = logging.getLogger(__name__)


def main(argv):
    options = 'htdi:o:'
    options_help = """ [-h, -t, -i <inputfile.json>] -o <outputdir>]
                   
                   -h   --help          Display this message.
                   -i   --inputfile     Provide the JSON input file.
                   -o   --outputdir     Set the output folder. Will be created if not found.
                   -d   --download      Download METASPACE study associated files. 
                   -t   --testmode      Read the input JSON file provided with option -i and print its content.
                   """

    input_file = ''
    output_dir = ''
    test_mode = False
    download_mode = False

    try:
        opts, args = getopt.getopt(argv, options, ['help', 'test', 'download', 'inputfile=', 'outputdir='])
    except getopt.GetoptError:
        print('Use: python ' + os.path.basename(sys.argv[0]) + options_help)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print('Read METASPACE JSON export file')
            print('Use: python ' + os.path.basename(sys.argv[0]) + options_help)
            sys.exit()
        if opt in ('-i', '--inputfile'):
            input_file = arg
        if opt in ('-o', '--outputdir'):
            output_dir = arg
        if opt in ('-d', '--download'):
            download_mode = True
        if opt in ('-t', '--test'):
            test_mode = True

    mtspc_obj = parse(input_file)

    if test_mode:
        print_mtspc_obj(mtspc_obj)
        exit(0)

    if download_mode:
        aws_download_files(mtspc_obj, output_dir)
        exit(0)


def print_mtspc_obj(mtspc_obj):
    for sample in mtspc_obj:
        for key, value in sample.items():
            print(key, value)
        print()


def aws_download_files(mtspc_obj, output_dir):
    credentials = configparser.ConfigParser()
    credentials.read(config.AWS_CREDENTIALS)
    mtspc = credentials['METASPACE']
    aws_bucket = mtspc['bucket'] + '/'

    for sample in mtspc_obj:
        imzml_file = get_imzml_filename(sample).replace(aws_bucket, '')
        path = imzml_file.split('/')
        folder = path[0]
        filename = path[1]
        logger.info("Getting file %s", imzml_file)

        imzml = aws_download_file(os.path.join(folder, filename), 'utf-8')
        if imzml is not None:
            save_file(imzml, output_dir, filename)

        ibd_file = get_ibd_filename(sample).replace(aws_bucket, '')
        path = ibd_file.split('/')
        folder = path[0]
        filename = path[1]
        logger.info("Getting file %s", ibd_file)

        ibd = aws_download_file(os.path.join(folder, filename))
        if ibd is not None:
            save_file(ibd, output_dir, filename, data_type='binary')


def parse(filename):
    assert os.path.exists(filename), "Did not find json input file: %s" % filename
    with open(filename, 'r', encoding='utf-8') as data_file:
        json_data = json.load(data_file)
    return json_data


def get_imzml_filename(sample_data):

    submitted_By = sample_data['Submitted_By']
    metaspace_options = sample_data['metaspace_options']
    Sample_Information = sample_data['Sample_Information']
    Additional_Information = sample_data['Additional_Information']
    MS_Analysis = sample_data['MS_Analysis']
    Sample_Preparation = sample_data['Sample_Preparation']
    s3dir = sample_data['s3dir']

    filename = metaspace_options['Dataset_Name'] + '.imzML'
    path = os.path.join(s3dir['imzML'].strip(filename), filename)
    # print(metaspace_options['Dataset_Name'], path)

    return path


def get_ibd_filename(submission):
    metaspace_options = submission['metaspace_options']
    s3dir = submission['s3dir']

    filename = metaspace_options['Dataset_Name'] + '.ibd'
    path = os.path.join(s3dir['ibd'].strip(filename), filename)

    return path


def save_file(content, path, filename, data_type='text'):

    if not os.path.exists(path):
        os.makedirs(path)

    mode = 'w'
    if data_type == 'binary':
        mode = 'wb'
    with open(os.path.join(path, filename), mode) as data_file:
        data_file.write(content)
        data_file.close()


if __name__ == "__main__":
   main(sys.argv[1:])
