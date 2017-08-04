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
    options = 'hi:o:'
    options_help = '[-h, -i <inputfile.json>] -o <outputdir>]'

    input_file = ''
    output_dir = ''

    try:
        opts, args = getopt.getopt(argv, options, ['inputfile=', 'outputdir='])
    except getopt.GetoptError:
        print('Use: python ' + os.path.basename(sys.argv[0]) + options_help)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('Read METASPACE JSON export file')
            print('Use: python ' + os.path.basename(sys.argv[0]) + options_help)
            sys.exit()
        if opt in ('-i', '--inputfile'):
            input_file = arg
        if opt in ('-o', '--output'):
            output_dir = arg

    credentials = configparser.ConfigParser()
    credentials.read(config.AWS_CREDENTIALS)
    mtspc = credentials['METASPACE']

    aws_bucket = mtspc['bucket'] + '/'

    submissions = read_josn_file(input_file)
    for submission in submissions:
        imzml_file = get_imzml_filename(submission).replace(aws_bucket, '')
        path = imzml_file.split('/')
        folder = path[0]
        filename = path[1]
        logger.info("Getting file %s", imzml_file)

        imzml = aws_download_file(os.path.join(folder, filename), 'utf-8')
        if imzml is not None:
            save_file(imzml, os.path.join(output_dir, folder), filename)

        ibd_file = get_ibd_filename(submission).replace(aws_bucket, '')
        path = ibd_file.split('/')
        folder = path[0]
        filename = path[1]
        logger.info("Getting file %s", ibd_file)

        ibd = aws_download_file(os.path.join(folder, filename))
        if ibd is not None:
            save_file(ibd, os.path.join(output_dir, folder), filename, data_type='binary')


def read_josn_file(file_name):
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data


def get_imzml_filename(submission):

    submitted_By = submission['Submitted_By']
    metaspace_options = submission['metaspace_options']
    Sample_Information = submission['Sample_Information']
    Additional_Information = submission['Additional_Information']
    MS_Analysis = submission['MS_Analysis']
    Sample_Preparation = submission['Sample_Preparation']
    s3dir = submission['s3dir']

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
