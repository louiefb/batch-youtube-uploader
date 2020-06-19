import os
import random
import time
import httplib2
import pickle

import pandas as pd
import google.oauth2.credentials
import google_auth_oauthlib.flow

from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

# Maximum number of times to retry before giving up.
MAX_RETRIES = 10

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the {{ Google Cloud Console }} at
# {{ https://cloud.google.com/console }}.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#     https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#     https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
CLIENT_SECRETS_FILE = 'client_secret.json'

# This OAuth 2.0 access scope allows an application to upload files to the
# authenticated user's YouTube channel, but doesn't allow other types of access.
SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

VALID_PRIVACY_STATUSES = ('public', 'private', 'unlisted')

# Authorize the request and store authorization credentials.
def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_console()
    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

def initialize_upload(youtube, options):
    tags = None
    body=dict(
        snippet=dict(
            title=options['title'],
            description=options['description'],
            tags=tags,
            categoryId=options['category']
        ),
        status=dict(
            privacyStatus=options['privacyStatus']
        )
    )

    # Call the API's videos.insert method to create and upload the video.
    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        # The chunksize parameter specifies the size of each chunk of data, in
        # bytes, that will be uploaded at a time. Set a higher value for
        # reliable connections as fewer chunks lead to faster uploads. Set a lower
        # value for better recovery on less reliable connections.
        #
        # Setting 'chunksize' equal to -1 in the code below means that the entire
        # file will be uploaded in a single HTTP request. (If the upload fails,
        # it will still be retried where it left off.) This is usually a best
        # practice, but if you're running on App Engine, you should set the
        # chunksize to something like
        # 1024 * 1024 (1 megabyte).
        media_body=MediaFileUpload(options['file'], chunksize=-1, resumable=True)
    )

    resumable_upload(insert_request, options['title'])

# This method implements an exponential backoff strategy to resume a
# failed upload.
def resumable_upload(request, title):
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            print(f'Uploading {title}...')
            status, response = request.next_chunk()
            if response is not None:
                if 'id' in response:
                    print(f'"{title}" was successfully uploaded.')
                else:
                    exit(f'The upload failed with an unexpected response: {response}')
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = f'A retriable HTTP error {e.resp.status} occurred:\n{e.content}'
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = f'A retriable error occurred: {e}'

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit('No longer attempting to retry.')

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f'Sleeping {sleep_seconds} seconds and then retrying...')
            time.sleep(sleep_seconds)

def gather_videos(pwd, title, description, begin_date, end_date, filetypes=['mp4']):
    """ Gathers videos from given pwd of given filetypes. Main title for each """
    """ video will be the title plus the date. Videos will be filtered by given """
    """ begin date and end date with the format of mm-dd-yy. Returns df of videos. """
    if os.path.isdir(pwd) is False:
            raise ValueError(f'{pwd} is not a valid directory')
    files = os.listdir(pwd)
    files = list(filter(lambda file: any([file.lower().endswith(filetype) for filetype in filetypes]), files))
    files = list(filter(lambda file: os.path.isfile(os.path.join(pwd, file)), files))

    paths = [os.path.join(pwd, file) for file in files]
    m_time = (os.path.getmtime(path) for path in paths)
    df = pd.DataFrame(list(zip(m_time, files, paths)))
    df.columns = ['time', 'file', 'path']

    dt_format = '%m-%d-%y'
    begin = datetime.timestamp(datetime.strptime(begin_date, dt_format))
    end = datetime.timestamp(datetime.strptime(end_date, dt_format))

    df = df[(df['time'] >= begin) & (df['time'] <= end)]

    df['date'] = df['time'].apply(lambda row: datetime.fromtimestamp(row).strftime(dt_format))
    df['title'] = df['date']
    df['description'] = str(description)

    for row in df.date.iteritems():
        count = list(df[df['date'] == row[1]].index)
        idx = count.index(row[0]) +1
        df['title'].loc[row[0]] = f'{title}, {row[1]} ({idx} of {len(count)})'

    return df

def upload(df, category=22, privacy='unlisted', rest=5, attempts=10):
    """ Uploads df of videos """
    options = dict()
    fail_count = 0

    youtube = get_authenticated_service()

    for row in df.itertuples():
        options['file'] = row.path
        options['title'] = row.title
        options['description'] = row.description
        options['category'] = int(category)
        options['privacyStatus'] = privacy
        while True and fail_count < attempts:
            try:
                initialize_upload(youtube, options)
                break
            except HttpError as e:
                fail_count += 1
                print(f'An HTTP error {e.resp.status} occurred:\n{e.content}\nTrying again in {rest} seconds...')
                time.sleep(rest)
        if fail_count >= attempts:
            df = df.loc[row.Index:, :]
            with open('df.pckl', 'wb'):
                pickle.dump(df, f)
            print(f'Uploading stopped due to max bad attempts reached. Remaining df pickled to "df.pckl".\nLast unsuccessful upload is "{options["title"]}".')
            break

if __name__ == '__main__':
    pwd = r'\Videos'
    filetypes = ['avi', 'mp4', '3gp', 'wmv', 'mov']

    title = 'My Vlog'
    begin_date = '08-23-18'
    end_date = '10-09-19'

    df = gather_videos(pwd, title, '', begin_date, end_date, filetypes)
    upload(df, 22, 'unlisted')
