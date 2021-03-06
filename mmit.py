import sys
import boto3
import requests
import config
import logging
import getopt
import os
import json
from aws_client import aws_download_file, AwsCredentials
from isa_api_client import IsaApiClient
from sm_annotation_utils.sm_annotation_utils import SMInstance
import csv
from collections import OrderedDict

msg_format = '%(asctime)s %(levelname)s %(message)s'
date_format= '%Y-%d-%m %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=date_format, level=logging.INFO)
logger = logging.getLogger(__name__)


def main(argv):
    short_options = 'hvti:o:patns:l'
    long_options = ['help', 'version', 'testmode',
                    'inputfile=', 'outputdir=',
                    'use-path',
                    'imzML', 'ibd', 'annotations', 'images',
                    'new-study', 'title=', 'description=',
                    'study-ids=',
                    'list-files'
                    ]
    options_help = """ [options]
    
General Options:
   -h   --help          Display this message.
   -v   --version       Display version information.
   -t   --testmode      Read the input JSON file provided with option -i and print its content.
   -s   --study-ids     Get Study JSON information. Input is a (comma separated) list of METASPACE identifiers.
   -i   --inputfile     Provide the JSON input file.
   -o   --outputdir     Set the output folder. Will be created if not found. 'output' will be used as default.
   -p   --use-path      Save files keeping same folder structure as in AWS  
        --imzML         Download *.imzml study associated files.
        --ibd           Download *.ibd study associated files.
        --annotations   Download JSON study file.
        --images        Download raw optical images.
   -a   --download-all  Download all associated files for a set of METASPACE Id's. Same as --imzML --idb --images --annotations.
   -n   --new-study     Create ISA-Tab new Study with provided title.
        --title         Study title.
        --description   Study description.
   -l   --list-files    List all files in AWS for a list of METASPACE identifiers.
"""

    input_file = ''
    output_dir = 'output'
    test_mode = False
    download_imzml = False
    download_ibd = False
    download_annotations = False
    download_images = False
    create_new_study = False
    std_title = ''
    std_description = ''
    use_path = False
    study_ids = list()
    download_all = False
    list_files = False

    try:
        opts, args = getopt.getopt(argv, shortopts=short_options, longopts=long_options)
    except getopt.GetoptError as err:
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
        if opt in ('-p', '--use-path'):
            use_path = True
        if opt in ('-s', '--study-ids'):
            study_ids = arg.split(',')
        if opt in ('-a', '--download-all'):
            download_all = True
        if opt in ('-l', '--list-files'):
            list_files = True

    if input_file:
        mtspc_obj = parse(input_file)

    if list_files:
        missing = list()
        if not study_ids:
            missing.append("-s --study-ids")
            print_need_additional_params(missing, options_help, exit_code=10)
        list_all_files(study_ids, ['.imzML', '.ibd', '.jpg', '.jpeg', '.png'])
        exit(0)

    if download_all:
        missing = list()
        if not study_ids:
            missing.append("-s --study-ids")
            print_need_additional_params(missing, options_help, exit_code=10)
        get_all_files(study_ids, ['.imzML', '.ibd', '.jpg', '.jpeg', '.png'], output_dir, use_path=use_path)
        exit(0)

    if study_ids:
        missing = list()
        if not std_title:
            missing.append("   --title")
            print_need_additional_params(missing, options_help, exit_code=11)
        get_study_json(study_ids, output_dir, std_title)

    if test_mode:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
            print_need_additional_params(missing, options_help, exit_code=12)
        print_mtspc_obj(mtspc_obj)
        exit(0)

    if download_imzml:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
            print_need_additional_params(missing, options_help, exit_code=13)
        aws_download_files(mtspc_obj, output_dir, 'imzML', data_type='utf-8', use_path=use_path)

    if download_ibd:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
            print_need_additional_params(missing, options_help, exit_code=14)
        aws_download_files(mtspc_obj, output_dir, 'ibd', data_type='binary', use_path=use_path)

    if download_annotations:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
            print_need_additional_params(missing, options_help, exit_code=15)
        aws_get_annotations(mtspc_obj, output_dir)

    if download_images:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
            print_need_additional_params(missing, options_help, exit_code=16)
        aws_get_images(mtspc_obj, output_dir, use_path=use_path)

    if create_new_study:
        missing = list()
        if not input_file:
            missing.append("-i --inputfile")
        if not std_title:
            missing.append("   --title")
        if not std_description:
            missing.append("   --description")
        if missing:
            print_need_additional_params(missing, options_help, exit_code=17)
        iac = IsaApiClient()
        inv = iac.new_study(std_title, std_description, mtspc_obj, output_dir, persist=True)
        print(inv)
        exit(0)


def print_need_additional_params(missing, options_help, exit_code=1):
    print()
    print('=> Missing required parameters:')
    for param in missing:
        print('\t', param)
    print()
    print('Usage: python ' + os.path.basename(sys.argv[0]) + options_help)
    exit(exit_code)


def print_mtspc_obj(mtspc_obj):
    for sample in mtspc_obj:
        for key, value in sample.items():
            print(key, value)
        print()


def aws_download_files(mtspc_obj, output_dir, extension, data_type='binary', use_path=False):
    for sample in mtspc_obj:
        aws_bucket, aws_path, file_name = get_filename_parts(sample, extension)
        logger.info("Getting file %s %s %s (%s)", aws_bucket, aws_path, file_name, data_type)
        file = aws_download_file(aws_bucket, aws_path, file_name, data_type)
        path = os.path.join(output_dir, aws_path) if use_path else output_dir
        if file:
            save_file(file, path, file_name, data_type)


def parse(filename):
    assert os.path.exists(filename), "Did not find json input file: %s" % filename
    with open(filename, 'r', encoding='utf-8') as data_file:
        json_data = json.load(data_file)
    return json_data


def get_filename_parts(sample_data, key):
    s3dir = sample_data['s3dir']
    value = s3dir[key]
    bucket_name = value.split('/')[0]
    file_name = value.split('/')[-1]
    aws_path = os.path.dirname(value).replace(bucket_name + '/', '')
    return bucket_name, aws_path, file_name


def save_file(content, path, filename, data_type='text'):
    if not os.path.exists(path):
        os.makedirs(path)
    mode = 'w'
    if data_type == 'binary':
        mode = 'wb'
    logger.info("Saving file %s %s (%s)", path, filename, data_type)
    with open(os.path.join(path, filename), mode) as data_file:
        data_file.write(content)


def aws_get_annotations(mtspc_obj, output_dir, database=config.DATABASE, fdr=config.FDR):

    filename = 'annotations'
    # CONNECT TO METASPACE SERVICES
    from sm_annotation_utils import sm_annotation_utils
    sm = SMInstance()  # connect to the main metaspace service
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

            annotations = OrderedDict([
                ('institution', institution),
                ('datasetName', dataset_name),
                ('formula', formula),
                ('adduct', adduct),
                ('mz', mz),
                ('msm', msm),
                ('fdr', fdr),
                ('rhoSpatial', rho_spatial),
                ('rhoSpectral', rho_spectral),
                ('rhoChaos', rho_chaos),
                ('moleculeNames', molecule_names)])

            # JSON file
            json_obj = json.dumps(annotations)
            save_file(json_obj, output_dir, filename + '.json', data_type='text')

            # Tab separated file
            with open(os.path.join(output_dir, filename + '.tsv'), "w") as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(annotations.keys())
                writer.writerow(annotations.values())
            f.close()

            return


aws_cred = AwsCredentials()


def aws_get_images(mtspc_obj, output_dir, use_path=False):
    sm = SMInstance()

    for sample in mtspc_obj:
        metaspace_options = sample['metaspace_options']
        ds_name = metaspace_options['Dataset_Name']
        ds = sm.dataset(name=ds_name)
        opt_im = ds._gqclient.getRawOpticalImage(ds.id)['rawOpticalImage']

        path = opt_im['url']
        img_url = ds._baseurl + path
        img_folder = os.path.dirname(path)
        img_name = os.path.basename(path)
        if img_name and not img_name == 'null':
            logger.info("Getting file %s", img_url)
            img_data = requests.get(img_url).content
            if img_data:
                out_path = output_dir + img_folder if use_path else output_dir
                save_file(content=img_data,
                          path=out_path,
                          filename=img_name + '.jpg', data_type='binary')


def get_aws_session(database):
    # CONNECT TO METASPACE SERVICES
    from sm_annotation_utils import sm_annotation_utils
    sm = SMInstance()  # connect to the main metaspace service
    db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

    return sm


def get_study_json(ds_ids, output_dir, std_title):

    session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
    s3 = session.resource('s3')
    sm = SMInstance()
    db = sm._moldb_client.getDatabase(config.DATABASE)
    std_json = []
    for ii, ds_id in enumerate(ds_ids):
        logger.info("Getting JSON information for %s", ds_id)
        ds = sm.dataset(id=ds_id)
        me = json.loads(ds.metadata.json)
        path = ds.s3dir[6:]  # strip s3a://
        bucket_name, ds_name = path.split('/', 1)
        bucket = s3.Bucket(bucket_name)
        me['s3dir'] = {}
        for obj in bucket.objects.filter(Prefix=path.split('/')[1]):
            if obj.key.endswith('.imzML'):
                me['s3dir']['imzML'] = path + "/" + obj.key.split('/')[-1]
            if obj.key.endswith('.ibd'):
                me['s3dir']['ibd'] = path + "/" + obj.key.split('/')[-1]
        std_json.append(me)

    save_file(json.dumps(std_json), output_dir, std_title + '.json', data_type='text')
    return std_json


def get_all_files(ds_ids, file_types, output_dir, use_path=False):

    session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
    s3 = session.resource('s3')
    sm = SMInstance()
    for ii, ds_id in enumerate(ds_ids):
        logger.info("Getting all files for %s", ds_id)
        ds = sm.dataset(id=ds_id)
        aws_path = ds.s3dir[6:]  # strip s3a://
        bucket_name, ds_name = aws_path.split('/', 1)
        aws_bucket = s3.Bucket(bucket_name)
        pref_filter = ds_name
        for obj in aws_bucket.objects.filter(Prefix=pref_filter):
            for suffix in file_types:
                if obj.key.endswith(suffix):
                    file_name = obj.key.split('/')[-1]
                    file = aws_download_file(bucket_name, ds_name, file_name)
                    if file:
                        out_path = os.path.join(output_dir, aws_path) if use_path else output_dir
                        save_file(file, out_path, file_name, data_type='binary')


def list_all_files(ds_ids, file_types):

    session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
    s3 = session.resource('s3')
    sm = SMInstance()
    for ii, ds_id in enumerate(ds_ids):
        logger.info("Getting all files for %s", ds_id)
        ds = sm.dataset(id=ds_id)
        aws_path = ds.s3dir[6:]  # strip s3a://
        bucket_name, ds_name = aws_path.split('/', 1)
        aws_bucket = s3.Bucket(bucket_name)
        pref_filter = ds_name
        for obj in aws_bucket.objects.filter(Prefix=pref_filter):
            for suffix in file_types:
                if obj.key.endswith(suffix):
                    print(bucket_name, obj.key)


if __name__ == "__main__":
    main(sys.argv[1:])
