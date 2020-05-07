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

# python3
"""Contains cloud authentication related functionality."""

import logging
from typing import List
from urllib import parse

BASE_URL = 'https://www.gstatic.com/bigquerydatatransfer/oauthz/auth'
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'


def retrieve_authorization_code(client_id: str, scopes: List[str],
                                app_name: str):
  """Returns authorization code.

  Args:
    client_id: The client id.
    scopes: The list of scopes.
    app_name: Name of the app.
  """
  scopes_str = ' '.join(scopes)
  authorization_code_request = {
      'client_id': client_id,
      'scope': scopes_str,
      'redirect_uri': REDIRECT_URI
  }

  encoded_request = parse.urlencode(
      authorization_code_request, quote_via=parse.quote)
  url = f'{BASE_URL}?{encoded_request}'
  logging.info(
      'Please click on the URL below to authorize %s and paste the '
      'authorization code.', app_name)
  logging.info('URL - %s', url)

  return input('Authorization Code : ')
