# coding=utf-8
# Copyright 2020 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manage operations on Cloud Storage."""

import os
from typing import Optional, Tuple
from urllib import parse

import logging

from google.api_core import exceptions
from google.api_core import retry
from google.cloud import storage
from plugins.cloud_utils import cloud_auth


class Error(Exception):
  """A generic error thrown for exceptions in cloud_storage module."""
  pass


class CloudStorageUtils(object):
  """CloudStorageUtils provides methods to manage Cloud Storage.

  Typical usage example:
       >>> cloud_storage = CloudStorageUtils('service_account_key_file.json')
       >>> cloud_storage.upload_file('/tmp/file.txt', 'gs://bucket_name')
  """

  def __init__(self,
               project_id: str,
               service_account_key_file: Optional[str] = None) -> None:
    """Initialize new instance of CloudStorageUtils.

    Args:
      project_id: GCP project id.
      service_account_key_file: File containing service account key. If not
        passed the default credential will be used. There are following ways to
        create service accounts -
          1) Use `build_service_client` method from `cloud_auth` module.
          2) Use `gcloud` command line utility as documented here -
               https://cloud.google.com/iam/docs/creating-managing-service-account-keys
    """
    credentials = cloud_auth.get_credentials(service_account_key_file)
    self.client = storage.Client(project=project_id, credentials=credentials)

  def _parse_blob_url(self, url: str) -> Tuple[str, str]:
    """"Parses Cloud Storage URL and returns a tuple of bucket name and path.

    Args:
      url: The full URL to blob, in the form of 'gs://bucket_name/path/to/blob'.

    Returns:
      Tuple: The bucket name and path tuple.

    Raises:
      Error: If the upload was not successful.
    """
    result = parse.urlparse(url)
    # result.path will be '/path/to/blob', we need to strip the leading '/'.
    bucket_name, path = result.hostname, result.path[1:]
    if not bucket_name:
      error_message = (f'Invalid URL - "{url}". The URL should be in the form '
                       f'of "gs://bucket_name/path/to/blob"')
      logging.exception(error_message)
      raise Error(error_message)
    return bucket_name, path

  @retry.Retry()
  def _get_or_create_bucket(self, bucket_name: str) -> storage.bucket.Bucket:
    """Retrieves or creates Cloud Storage bucket given bucket name.

    If the bucket doesn't exist in the Cloud Storage, it will be created and the
    newly created bucket will be returned. The operation will be retried for
    transient errors e.g. - google.api_core.exceptions.InternalServerError.

    Args:
      bucket_name: Name of the bucket to be retieved.

    Returns:
      google.cloud.storage.bucket.Bucket: The bucket object.
    """
    try:
      return self.client.get_bucket(bucket_name)
    except exceptions.NotFound:
      logging.info(f'Cloud Storage bucket "{bucket_name}" not found. '
                   'Hence creating the bucket.')
      bucket = self.client.create_bucket(bucket_name)
      logging.info(
          f'Cloud Storage bucket "{bucket_name}" created successfully.')
      return bucket

  def upload_file_to_url(self, source_file_path: str,
                         destination_file_url: str):
    """Uploads file from source file system to Cloud Storage.

    This is a convenience method that parses bucket name and path, and calls
    upload_file, so that the client can pass the full path as a whole.

    Args:
      source_file_path: Path to the file to be uploaded. e.g - /tmp/file.txt
      destination_file_url: The full URL to destination file, in the form of
        'gs://bucket_name/path/to/file'.
    """
    bucket_name, path = self._parse_blob_url(destination_file_url)
    self.upload_file(source_file_path, bucket_name, path)

  def upload_file(self, source_file_path: str, bucket_name: str,
                  destination_file_path: str) -> None:
    """Uploads file from source file system to Cloud Storage.

    If the bucket doesn't exist in the Cloud Storage, it will be created.

    Args:
      source_file_path: Path to the file to be uploaded. e.g - /tmp/file.txt
      bucket_name: Cloud Storage bucket to which the file should be uploaded. If
        the Cloud Storage URL is 'gs://bucket1/file1.txt', then the bucket_name
        would be 'bucket1'.
      destination_file_path: Path of the destination blob/object within the
        Cloud Storage bucket. If the Cloud Storage URL is
        'gs://bucket1/dir1/file1.txt', then the destination_file_path would be
        'dir1/file1.txt'.
    Raises:
      FileNotFoundError: If the provided file is not found.
      Error: If the upload was not successful.
    """
    if not os.path.isfile(source_file_path):
      logging.error(f'The file "{source_file_path}" could not be found.')
      raise FileNotFoundError(
          f'The file "{source_file_path}" could not be found.')
    try:
      logging.info('Uploading "%s" file to "gs://%s/%s"', source_file_path,
                   bucket_name, destination_file_path)
      bucket = self._get_or_create_bucket(bucket_name)
      self._upload_file(source_file_path, bucket, destination_file_path)
      logging.info('Uploaded "%s" file to "gs://%s/%s"', source_file_path,
                   bucket_name, destination_file_path)
    except exceptions.RetryError:
      error_message = (f'Error when uploading file "{source_file_path}" to '
                       f'"gs://{bucket_name}/{destination_file_path}"')
      logging.exception(error_message)
      raise Error(error_message)

  @retry.Retry()
  def _upload_file(self, source_file_path: str, bucket: storage.bucket.Bucket,
                   destination_file_path: str) -> None:
    """Uploads file to Cloud Storage with Retry logic.

    The Retry decorator will retry transient API errors. Following errors are
    some examples of transient errors:
    1. google.api_core.exceptions.InternalServerError
    2. google.api_core.exceptions.TooManyRequests
    3. google.api_core.exceptions.ServiceUnavailable

    Args:
      source_file_path: Path to the file to be uploaded. e.g - /tmp/file.txt
      bucket: Cloud Storage bucket to which the file should be uploaded.
      destination_file_path: Path of the destination blob/object within the
        Cloud Storage bucket. If the Cloud Storage URL is
        'gs://bucket1/dir1/file1.txt', then the destination_file_path would be
        'dir1/file1.txt'.
    """
    destination_blob = bucket.blob(destination_file_path)
    destination_blob.upload_from_filename(source_file_path)

  def upload_directory_to_url(self, source_directory_path: str,
                              destination_dir_url: str):
    """Uploads an entire directory to Cloud Storage.

    This is a convenience method that parses bucket name and path, and calls
    upload_directory, so that the client can pass the full path as a whole.

    Args:
        source_directory_path: Path to the directory to be uploaded: e.g -
        /tmp/dir1/dir2
      destination_dir_url: The full URL to destination directory, in the form of
        'gs://bucket_name/path/to/dir'.
    """
    bucket_name, path = self._parse_blob_url(destination_dir_url)
    self.upload_directory(source_directory_path, bucket_name, path)

  def upload_directory(self, source_directory_path: str, bucket_name: str,
                       destination_dir_path: str) -> None:
    """Uploads an entire directory to Cloud Storage.

    All the files in the source directory are identified recursively and
    uploaded to Cloud Storage bucket. The symlinks in the source directory is
    ignored to avoid infinite recursion. If the bucket doesn't exist in the
    Cloud Storage it will be created.

    Args:
      source_directory_path: Path to the directory to be uploaded: e.g -
        /tmp/dir1/dir2
      bucket_name: Cloud Storage bucket to which the directory should be
        uploaded. If the Cloud Storage URL is 'gs://bucket1/dir1/dir2', then the
        bucket_name would be 'bucket1'.
      destination_dir_path: Path of the destination blob/object within the
        Cloud Storage bucket. If the Cloud Storage URL is
        'gs://bucket/dir1/dir2', then the destination_dir_path would be
        'dir1/dir2'.

    Raises:
      FileNotFoundError: If the provided directory is not found.
    """
    if not os.path.isdir(source_directory_path):
      logging.error(
          f'The directory "{source_directory_path}" could not be found.')
      raise FileNotFoundError(
          f'The directory "{source_directory_path}" could not be found.')
    logging.info('Uploading "%s" directory to "gs://%s/%s"',
                 source_directory_path, bucket_name, destination_dir_path)
    files_to_upload = []
    for (root, _, files) in os.walk(source_directory_path):
      if not files:
        continue
      for file in files:
        full_path = os.path.join(root, file)
        files_to_upload.append(full_path)
    bucket = self._get_or_create_bucket(bucket_name)
    for file in files_to_upload:
      # Construct destination path by replacing source directory path:
      # If the source directory is `/tmp/dir1` and destination_dir_path is
      # `obj1/obj2` then file `/tmp/dir1/dir2/file.txt` will have a destination
      # file path `obj1/obj2/dir2/file.txt`
      destination_file_path = file.replace(source_directory_path,
                                           destination_dir_path)
      self._upload_file(file, bucket, destination_file_path)
    logging.info('Uploaded "%s" directory to "gs://%s/%s"',
                 source_directory_path, bucket_name, destination_dir_path)

  def write_to_path(self, file_content: str, destination_file_path: str):
    """Writes file content to Cloud Storage file.

    This is a convenience method that parses bucket name and path, and calls
    write_to_file, so that the client can pass the full path as a whole.

    Args:
      file_content: The content to be written to the file.
      destination_file_path: The full path to destination file, in the form of
        'gs://bucket_name/path/to/file'.
    """
    bucket_name, path = self._parse_blob_url(destination_file_path)
    self.write_to_file(file_content, bucket_name, path)

  @retry.Retry()
  def write_to_file(self, file_content: str, bucket_name: str,
                    destination_file_path: str):
    """Writes file content to Cloud Storage file.

    If the bucket doesn't exist in the Cloud Storage, it will be created. If the
    file already exists in the Cloud Storage, the content of the file will be
    overwritten. The operation will be retried for transient errors e.g. -
    google.api_core.exceptions.InternalServerError.

    Args:
      file_content: The content to be written to the file.
      bucket_name: Cloud Storage bucket to which the content should be written.
        If the Cloud Storage URL is 'gs://bucket1/file1.txt', then the
        bucket_name would be 'bucket1'.
      destination_file_path: Path of the destination blob/object within the
        Cloud Storage bucket. If the Cloud Storage URL is
        'gs://bucket1/dir1/file1.txt', then the destination_file_path would be
        'dir1/file1.txt'.
    """
    logging.info('Writing data to "gs://%s/%s"', bucket_name,
                 destination_file_path)
    bucket = self._get_or_create_bucket(bucket_name)
    destination_blob = bucket.blob(destination_file_path)
    destination_blob.upload_from_string(file_content)
    logging.info('Successfully wrote data to "gs://%s/%s"', bucket_name,
                 destination_file_path)
