import logging
import config
import configparser
import boto3

msg_format = '%(asctime)s %(levelname)s %(message)s'
date_format= '%Y-%d-%m %H:%M:%S'
logging.basicConfig(format=msg_format, datefmt=date_format, level=logging.INFO)
logger = logging.getLogger(__name__)


class AwsCredentials(object):

    def __init__(self):
        self.aws_access_key_id = ''
        self.aws_secret_access_key = ''
        self.bucket = ''

        self.read_aws_credentials()
        return

    def read_aws_credentials(self):
        credentials = configparser.ConfigParser()
        credentials.read(config.AWS_CREDENTIALS)
        account = credentials['METASPACE']

        self.aws_access_key_id = account['access_key_id']
        self.aws_secret_access_key = account['secret_access_key']
        self.bucket = account['bucket']

    @property
    def get_access_key(self):
        return self.aws_access_key_id

    @property
    def get_secret_access_key(self):
        return self.aws_secret_access_key

    @property
    def get_bucket(self):
        return self.bucket


aws_cred = AwsCredentials()
session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
s3 = session.resource('s3')
bucket = s3.Bucket(aws_cred.get_bucket)


def aws_download_file(source, data_type='binary'):

    obj = bucket.Object(source)
    logger.info("Downloading %s", source)
    body = None
    try:
        if data_type == 'utf-8':
            body = obj.get()['Body'].read().decode('utf-8')
        else:
            body = obj.get()['Body'].read()
    except Exception:
        logger.warning("Failed to download %s", source)

    return body
