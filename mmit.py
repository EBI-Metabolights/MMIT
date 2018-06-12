import sys
import boto3
import requests
import config
import configparser
import logging
import getopt
import os
import json
from aws_client import aws_download_file, AwsCredentials
from isa_api_client import IsaApiClient
from sm_annotation_utils.sm_annotation_utils import SMInstance

msg_format = '%(asctime)s %(levelname)s %(message)s'
date_format= '%Y-%d-%m %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=date_format, level=logging.INFO)
logger = logging.getLogger(__name__)


def main(argv):
    short_options = 'hvti:o:a:tn'
    long_options = ['help', 'version', 'testmode',
                    'inputfile=', 'outputdir=',
                    'imzML', 'ibd', 'annotations', 'images',
                    'new-study', 'title=', 'description='
                    ]
    options_help = """ [options]
    
General Options:
   -h   --help          Display this message.
   -v   --version       Display version information.
   -t   --testmode      Read the input JSON file provided with option -i and print its content.
   -i   --inputfile     Provide the JSON input file.
   -o   --outputdir     Set the output folder. Will be created if not found. 
        --imzML         Download *.imzml study associated files.
        --ibd           Download *.ibd study associated files.
        --annotations   Download JSON study file.
        --images        Download raw optical images.
   -n   --new-study     Create ISA-Tab new Study with provided title.
        --title         Study title.
        --description   Study description.
"""

    input_file = ''
    output_dir = ''
    test_mode = False
    download_imzml = False
    download_ibd = False
    download_annotations = False
    download_images = False
    create_new_study = False
    std_title = ''
    std_description = ''

    try:
        opts, args = getopt.getopt(argv, shortopts=short_options, longopts=long_options)
    except getopt.GetoptError:
        print('Usage: python ' + os.path.basename(sys.argv[0]) + options_help)
        sys.exit(2)
    if len(opts) < 1:
        print(config.APP_NAME, config.APP_VERSION)
        print('Usage: python ' + os.path.basename(sys.argv[0]) + options_help)
        exit()
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(config.APP_NAME, config.APP_VERSION)
            print(config.APP_DESCRIPTION)
            print('Use: python ' + os.path.basename(sys.argv[0]) + options_help)
            exit()
        if opt in ('-v', '--version'):
            print(config.APP_NAME, config.APP_VERSION)
            exit()
        if opt in ('-i', '--inputfile'):
            input_file = arg
        if opt in ('-o', '--outputdir'):
            output_dir = arg
        if opt in ('-t', '--testmode'):
            test_mode = True
        if opt == '--imzML':
            download_imzml = True
        if opt == '--ibd':
            download_ibd = True
        if opt == '--annotations':
            download_annotations = True
        if opt == '--images':
            download_images = True
        if opt in ('-n', '--new-study'):
            create_new_study = True
        if opt == '--title':
            std_title = arg
        if opt == '--description':
            std_description = arg

    mtspc_obj = parse(input_file)

    if test_mode:
        print_mtspc_obj(mtspc_obj)
        exit(0)

    if download_imzml:
        aws_download_files(mtspc_obj, output_dir, 'imzML', data_type='utf-8')
    if download_ibd:
        aws_download_files(mtspc_obj, output_dir, 'ibd', data_type='binary')
    if download_annotations:
        aws_get_annotations(mtspc_obj, output_dir)
    if download_images:
        aws_get_images(mtspc_obj, output_dir)
    if create_new_study:
        iac = IsaApiClient()
        inv = iac.new_study(std_title, std_description, mtspc_obj, output_dir, persist=True)
        print(inv)


def print_mtspc_obj(mtspc_obj):
    for sample in mtspc_obj:
        for key, value in sample.items():
            print(key, value)
        print()


def aws_download_files(mtspc_obj, output_dir, extension, data_type='binary'):
    credentials = configparser.ConfigParser()
    credentials.read(config.AWS_CREDENTIALS)
    mtspc = credentials['METASPACE']
    aws_bucket = mtspc['bucket'] + '/'

    for sample in mtspc_obj:
        a_file = get_filename(sample, extension).replace(aws_bucket, '')
        path = a_file.split('/')
        folder = path[0]
        filename = path[1]
        logger.info("Getting file %s", a_file)
        file = aws_download_file(os.path.join(folder, filename), data_type)
        if file:
            save_file(file, output_dir, filename, data_type)


def parse(filename):
    assert os.path.exists(filename), "Did not find json input file: %s" % filename
    with open(filename, 'r', encoding='utf-8') as data_file:
        json_data = json.load(data_file)
    return json_data


def get_filename(sample_data, extension):
    metaspace_options = sample_data['metaspace_options']
    s3dir = sample_data['s3dir']
    filename = metaspace_options['Dataset_Name'] + '.' + extension
    path = os.path.join(s3dir[extension].strip(filename), filename)
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


def aws_get_annotations(mtspc_obj, output_dir, database=config.DATABASE, fdr=config.FDR):
    # CONNECT TO METASPACE SERVICES
    from sm_annotation_utils import sm_annotation_utils
    sm = sm_annotation_utils.SMInstance()  # connect to the main metaspace service
    db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

    for sample in mtspc_obj:
        metaspace_options = sample['metaspace_options']
        ds_name = metaspace_options['Dataset_Name']
        ds = sm.dataset(name=ds_name)
        # print('Dataset name: ', ds_name)
        # print('Dataset id: ', ds.id)
        # print('Dataset config: ', ds.config)
        # print('Dataset DBs: ', ds.databases)
        # print('Dataset adducts: ', ds.adducts)
        # print('Dataset metadata: ', ds.metadata.json)
        # print('Dataset polarity: ', ds.polarity)
        # print('Dataset results: ', ds.results())

        print()

        for an in ds.annotations(fdr=fdr, database=database):
            # print(an)

            nms = db.names(an[0])
            # print(nms)

            ids = db.ids(an[0])
            # print(ids)

            img = ds.isotope_images(sf=an[0], adduct=an[1])[0]  # get image for this molecule's principle peak
            mii = img[img > 0].mean()  # mean image intensity

            institution = sample['Submitted_By']['Institution']
            dataset_name = ds_name
            formula = an[0]
            adduct = ds.adducts[0]
            mz = ''
            msm = str(mii)
            fdr = ''
            rho_spatial = ''
            rho_spectral = ''
            rho_chaos = ''
            molecule_names = nms

            print('"institution"	"datasetName"	"formula"	"adduct"	"mz"	"msm"	'
                  '"fdr"	"rhoSpatial"	"rhoSpectral"	"rhoChaos"	"moleculeNames"')
            print(institution, dataset_name, formula, adduct, mz, msm,
                  fdr, rho_spatial, rho_spectral, rho_chaos, molecule_names)

            items = [institution, dataset_name, formula, adduct, mz, msm,
                     fdr, rho_spatial, rho_spectral, rho_chaos, molecule_names]
            print(*items, sep='\t')
            return


aws_cred = AwsCredentials()


def aws_get_images(mtspc_obj, output_dir):

    session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
    s3 = session.resource('s3')
    sm = SMInstance()

    for sample in mtspc_obj:
        metaspace_options = sample['metaspace_options']
        ds_name = metaspace_options['Dataset_Name']
        ds = sm.dataset(name=ds_name)
        opt_im = ds._gqclient.getRawOpticalImage(ds.id)['rawOpticalImage']
        img_url = ds._baseurl + opt_im['url']
        img_folder = opt_im['url'].split('/')[1]
        img_name = opt_im['url'].split('/')[2]

        logger.info("Getting file %s", img_url)
        img_data = requests.get(img_url).content
        if img_data:
            save_file(content=img_data,
                      path=os.path.join(output_dir, img_folder),
                      filename=img_name + '.jpg', data_type='binary')


def get_aws_session(database):
    # CONNECT TO METASPACE SERVICES
    from sm_annotation_utils import sm_annotation_utils
    sm = sm_annotation_utils.SMInstance()  # connect to the main metaspace service
    db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

    return sm


if __name__ == "__main__":
   main(sys.argv[1:])
