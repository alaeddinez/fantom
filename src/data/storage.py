# ======================== import ===================================
from google.cloud import storage as storage
import pandas as pd
import io
class storage_blob:
    def __init__(self, bucket, blob):
        self.my_client = storage.Client()
        self.bucket = self.my_client.get_bucket(bucket)
        self.blob = self.bucket.get_blob(blob)
    def select_bucket(self, sep=";") -> pd.DataFrame:
        #DF = pd.read_csv(io.StringIO(self.blob.download_as_string().decode('utf-8')), sep=sep)
        DF_vekia = pd.read_csv(io.StringIO(self.blob.download_as_string().decode('utf-8')), sep=sep, header=0,
                            skipinitialspace=True,
                            skiprows=[1])
        return(DF_vekia)

    def upload_blob(self, source_file_name):
        """Uploads a file to the bucket."""
        blob.upload_from_filename(source_file_name)
        print('File {} uploaded to {}.'.format(
            source_file_name,
            destination_blob_name))
    def delete_blob(self):
        """Deletes a blob from the bucket."""
        self.blob.delete()
        print('Blob {} deleted.'.format(blob_name))
    def blob_metadata(self):
        """Prints out a blob's metadata."""
        blob = self.blob
        print('Blob: {}'.format(blob.name))
        print('Bucket: {}'.format(blob.bucket.name))
        print('Storage class: {}'.format(blob.storage_class))
        print('ID: {}'.format(blob.id))
        print('Size: {} bytes'.format(blob.size))
        print('Updated: {}'.format(blob.updated))
        print('Generation: {}'.format(blob.generation))
        print('Metageneration: {}'.format(blob.metageneration))
        print('Etag: {}'.format(blob.etag))
        print('Owner: {}'.format(blob.owner))
        print('Component count: {}'.format(blob.component_count))
        print('Crc32c: {}'.format(blob.crc32c))
        print('md5_hash: {}'.format(blob.md5_hash))
        print('Cache-control: {}'.format(blob.cache_control))
        print('Content-type: {}'.format(blob.content_type))
        print('Content-disposition: {}'.format(blob.content_disposition))
        print('Content-encoding: {}'.format(blob.content_encoding))
        print('Content-language: {}'.format(blob.content_language))
        print('Metadata: {}'.format(blob.metadata))
        print("Temporary hold: ",
              'enabled' if blob.temporary_hold else 'disabled')
        print("Event based hold: ",
              'enabled' if blob.event_based_hold else 'disabled')
        if blob.retention_expiration_time:
            print("retentionExpirationTime: {}".format(blob.retention_expiration_time))
    def rename_blob(self, new_name):
        """Renames a blob."""
        new_blob = self.bucket.rename_blob(self.blob, new_name)
        print('Blob {} has been renamed to {}'.format(self.blob.name, new_blob.name))
def create_bucket(bucket_name):
    """Creates a new bucket."""
    storage_client = storage.Client()
    bucket = storage_client.create_bucket(bucket_name)
    print('Bucket {} created'.format(bucket.name))
def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()
    print('Bucket {} deleted'.format(bucket.name))
def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    for blob in blobs:
        print(blob.name)
def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        /a/1.txt
        /a/b/2.txt
    If you just specify prefix = '/a', you'll get back:
        /a/1.txt
        /a/b/2.txt
    However, if you specify prefix='/a' and delimiter='/', you'll get back:
        /a/1.txt
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    print('Blobs:')
    for blob in blobs:
        print(blob.name)
    if delimiter:
        print('Prefixes:')
        for prefix in blobs.prefixes:
            print(prefix)
# import os
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/kiki/Documents/connaissance-client-c6d4111ee8c4.json'
#
# storage_blob(bucket='conn-cli-di-lmfr', blob='tmp_MARGE.csv').select_bucket(sep=",")