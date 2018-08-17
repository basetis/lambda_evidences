"""
Script to upload code evidences to google drive every first day of the month
"""

import json
import os
import zipfile
from datetime import datetime, timedelta
import boto3

from slackclient import SlackClient
import requests


EMAILS = {
    'arnau.villoro@basetis.com': 'Arnau_Villoro',
    'villoro7@gmail.com': 'Arnau_Villoro',
    'juanjo.ojeda@basetis.com': 'Juanjo_Ojeda',
    'gustau.sole@basetis.com': 'Gustau_Sole',
    'guztavo.sole@gmail.com': 'Gustau_Sole',
    'guillem.olive@basetis.com': 'Guillem_Olive',
    'cristina.perezrendon@basetis.com': 'Cristina_PerezRendon',
    'daniel.vergara@basetis.com': 'Daniel_Vergara',
    'elisabet.pino@basetis.com': 'Elisabet_Pino',
    'nikolina.krizanec@basetis.com': 'Nikolina_Krizanec',
    'roger.calvo@basetis.com': 'Roger_Calvo',
    'roger.romero@basetis.com': 'Roger_Romero',
    'tomas.ortega@basetis.com': 'Tomas_Ortega',
    'gerardo.reichl@basetis.com': 'Gerardo_Reichl',
    'victor.garciab@basetis.com': 'Victor_Garcia',
    'xavi.gutierrez@basetis.com': 'Xavi_Gutierrez'
}

PEOPLE = list(set(EMAILS.values()))

REPS = [
    {
        'name': 'AMM_Reporting',
        'owner': 'BaseTIS'
    },
    {
        'name': 'sma_id',
        'owner': 'BaseTIS'
    },
    {
        'name': 'dash-log',
        'owner': 'sm-analytics'
    },
    {
        'name': 'dash_sma',
        'owner': 'sm-analytics'
    },
    {
        'name': 'sma_slack',
        'owner': 'BaseTIS'
    },
    {
        'name': 'sma_welcome',
        'owner': 'BaseTIS'
    },
    {
        'name': 'AMM_Calidad_Tecnica',
        'owner': 'BaseTIS'
    }
]

BUCKET_NAME = 'basetis-services'
BUCKET_URI = 'am-managers/test/Imputació d\'hores/{date:%Y_%m}/{date:%Y_%m}-{}.zip'

SLACK_MESSAGES = {
    True: {
        True: "*{mdate:%B}* evidences succesfully uploaded to S3 and Google Drive.",
        False: """*{mdate:%B}* evidences partially uploaded to S3 and Google Drive.
There are commits made by people that I don't recognize: {emails}
Please update my EMAILS dict and run me again."""
    },
    False: {
        True: """*{mdate:%B}* evidences partially uploaded to S3 and Google Drive.
There are repositories that I don't recognize: {reps}
Please update my REPS list and run me again.
""",
        False: """*{mdate:%B}* evidences partially uploaded to S3 and Google Drive.
There are repositories that I don't recognize: {reps}
There are commits made by people that I don't recognize: {emails}
Please update my REPS list and my EMAILS dict and run me again."""
    }
}


def get_month_range(mdate=datetime.now()):
    """
        Returns two datetimes:
            - The first one is the first day of mdate's month
            - The second one is the first day of mdate's next month
    """

    year = mdate.year
    month = mdate.month

    return datetime(year, month, 1), datetime(year, month + 1, 1)


def parse_author(author):
    """
        Returnts the email of the author of a commit
    """

    email = author.split('<')[1]
    return email.replace('>', '')


def parse_date(datestring):
    """
        Returns the datetime represented by datestring
    """

    datestring = datestring.split('+')[0]
    mformat = '%Y-%m-%dT%X'
    return datetime.strptime(datestring, mformat)


def get_bitbucket_token():
    """
        Returns a bitbucket token to connect via API
    """

    data = {'grant_type': 'client_credentials'}

    response = requests.post(
        'https://bitbucket.org/site/oauth2/access_token',
        auth=(os.environ['BITBUCKET_KEY_ID'], os.environ['BITBUCKET_SECRET_KEY']),
        data=data
    )

    return json.loads(response.text)['access_token']


def get_evidences(repository, mdate=datetime.now()):
    """
        Returns a dict with all the evidences of mdate's month in the given repository
    """

    first_day, last_day = get_month_range(mdate)

    baseurl = 'https://bitbucket.org/api/2.0/repositories/{}/{}/commits/'
    urlcommits = baseurl.format(repository['owner'], repository['name'])
    headers = {"Authorization": "Bearer {}".format(get_bitbucket_token())}

    return_dict = {}
    rep_not_found = False

    # Primer arribo a la pagina on comença el mes
    while True:
        response = requests.get(urlcommits, headers=headers)

        if response:
            text = json.loads(response.text)

            commit_date = parse_date(text['values'][-1]['date'])
            if commit_date < last_day:
                break


            urlcommits = text.get('next', None)
            if urlcommits is None:
                break

        else:
            urlcommits = None
            rep_not_found = True
            break

    passed_month = False

    while not passed_month and urlcommits is not None:
        response = requests.get(urlcommits, headers=headers)
        text = json.loads(response.text)

        for commit in text['values']:
            commit_date = parse_date(commit['date'])
            if commit_date < first_day:
                passed_month = True
                break

            if commit_date < last_day:
                commit_id = commit['hash']
                return_dict[commit_id] = {
                    'author': commit['author']['raw'],
                    'date': commit['date'],
                    'message': commit['message']
                }

                return_dict[commit_id]['email'] = parse_author(commit['author']['raw'])

        urlcommits = text.get('next', None)

    return return_dict, rep_not_found


def get_approvers(message):
    """
        Returns a list with the approvers of a commit (empty list if it's not a pull request)
    """

    if not 'pull request' in message:
        return []

    return [x.split('>')[0] for x in message.split('<')[1:]]


def evidences_by_person(evidences):
    """
        Returns a dict with the evidences for every person in PEOPLE, and a bool
        that is True if and only if all the emails appearing in the commits are in EMAILS
    """

    not_found_emails = []

    ev_by_per = {}
    for person in PEOPLE:
        ev_by_per[person] = []

    for com_id in evidences:
        commit = evidences[com_id]
        commit['hash'] = com_id
        email = commit['email']

        if email in EMAILS:
            ev_by_per[EMAILS[email]].append(commit)
        elif not email in not_found_emails:
            not_found_emails.append(email)

        for approver in get_approvers(commit['message']):
            if approver in EMAILS:
                ev_by_per[EMAILS[approver]].append(commit)
            elif not email in not_found_emails:
                not_found_emails.append(approver)

    return ev_by_per, not_found_emails


def check_if_uri_exist(uris):
    """
        Checks if a path exists, and if not creates it
    """

    # If is not a list, make a list in order to iterate
    if not isinstance(uris, list):
        uris = [uris]

    for uri in uris:
        # Check that it is not a file without path
        if len(uri.split("/")) > 1:
            uri = uri.rsplit('/', 1)[0]

            # If the directory doesn't exist, it will be created.
            if not os.path.isdir(uri):
                os.makedirs(uri, exist_ok=True)


def create_evidences(ev_by_per, mdate, rep, path='/tmp/Imputació d\'hores/'):
    """
        Creates a file for each person appearing in ev_by_per, and writes the evidences
        of each person in the corresponding file
    """

    folder = "{:%Y_%m}".format(mdate)

    check_if_uri_exist(path + folder + '/')

    for person in ev_by_per:
        file_name = folder + '_' + rep + '-' + person + '.txt'
        ev_person = ev_by_per[person]

        if ev_person:
            with open(path + folder + '/' + file_name, 'w') as file:
                for commit in ev_person:
                    file.write("\n".join([
                        'commit ' + commit['hash'],
                        'author: ' + commit['author'],
                        'date: ' + commit['date'] + '\n',
                        commit['message'] + '\n'
                    ]))


def create_zips(mdate=datetime.now()):
    """
        For each person in PEOPLE, creates a zip with the evidences of all the repositories
    """

    folder = '/tmp/{:%Y_%m}-{}.zip'
    path = '/tmp/Imputació d\'hores/{:%Y_%m}/'.format(mdate)
    file = '{:%Y_%m}_{}-{}.txt'

    for person in PEOPLE:
        with zipfile.ZipFile(folder.format(mdate, person), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for rep in REPS:
                file_name = file.format(mdate, rep['name'], person)
                path_and_file = os.path.join(path, file_name)
                if os.path.isfile(path_and_file):
                    zipf.write(path_and_file, arcname=file_name)


def get_s3_client():
    """
        Returns an s3 client
    """

    return boto3.client(
        's3',
        aws_access_key_id=os.environ['S3_KEY_ID'],
        aws_secret_access_key=os.environ['S3_SECRET_KEY']
    )


def upload_to_s3(mdate=datetime.now()):
    """
        Uploads the zips to s3
    """

    file = '/tmp/{:%Y_%m}-{}.zip'

    s3_client = get_s3_client()

    for person in PEOPLE:
        s3_client.upload_file(
            file.format(mdate, person),
            BUCKET_NAME,
            BUCKET_URI.format(person, date=mdate)
        )


def get_drive_token():
    """
        Returns a Google Drive token to connect via API
    """

    data = {
        'grant_type': 'refresh_token',
        'client_id': os.environ['GOOGLE_KEY_ID'],
        'client_secret': os.environ['GOOGLE_SECRET_KEY'],
        'refresh_token': os.environ['GOOGLE_REFRESH_TOKEN']
    }

    response = requests.post(
        'https://accounts.google.com/o/oauth2/token',
        data=data,
    )

    return json.loads(response.text)['access_token']


def delete_previous_folders(token, mdate=datetime.now()):
    """
        Deletes previous folders with the desired evidences
    """

    folder_name = "{:%Y_%m}".format(mdate)

    url = "https://www.googleapis.com/drive/v2/files?q=title='{}'"
    headers = {"Authorization": "Bearer {}".format(token)}
    response = requests.get(url.format(folder_name), headers=headers)

    for folder in json.loads(response.text)['items']:
        folder_id = folder['id']

        for parent in folder['parents']:
            if parent['id'] == '1Tg--xIaCy4z_3sewqpXQbsOWXAHPdZ-X':
                requests.delete(
                    'https://www.googleapis.com/drive/v2/files/{}'.format(folder_id),
                    headers=headers
                )


def create_drive_folder(token, mdate=datetime.now()):
    """
        Creates a folder in Google Drive for the given month
    """

    headers = {"Authorization": "Bearer {}".format(token)}
    params = {
        "name": "{:%Y_%m}".format(mdate),
        'mimeType': 'application/vnd.google-apps.folder',
        "parents": ["1Tg--xIaCy4z_3sewqpXQbsOWXAHPdZ-X"],
    }
    files = {
        'data': ('metadata', json.dumps(params), 'application/json; charset=UTF-8'),
    }

    response = requests.post(
        "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
        headers=headers,
        files=files
    )

    return json.loads(response.text)['id']


def upload_drive_zips(token, folder_id, mdate=datetime.now()):
    """
        Uploads all the zips to the folder created in Google Drive
    """

    file = '{:%Y_%m}-{}.zip'
    file_local = '/tmp/{:%Y_%m}-{}.zip'
    headers = {"Authorization": "Bearer {}".format(token)}

    for person in PEOPLE:
        para = {
            "name": file.format(mdate, person),
            "parents": [folder_id]
        }
        files = {
            'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
            'file': open(file_local.format(mdate, person), "rb")
        }

        requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers=headers,
            files=files
        )


def upload_to_drive(mdate=datetime.now()):
    """
        Wrapper to upload the zips to Google Drive
    """

    token = get_drive_token()
    delete_previous_folders(token, mdate)
    folder_id = create_drive_folder(token, mdate)
    upload_drive_zips(token, folder_id, mdate)


def get_slack_client():
    """
        Returns a Slack Client with the enviornment token
    """

    token = os.environ.get('BOT_TOKEN')
    if token is None:
        return False

    return SlackClient(token)


def send_slack_message(not_found_emails_in_all_reps=None,
                       mdate=datetime.now(),
                       reps_not_found=None):
    """
        It sends a slack message with information about the process
    """

    slack_client = get_slack_client()

    all_reps_found = not bool(reps_not_found)
    all_emails_in_all_reps = not bool(not_found_emails_in_all_reps)

    text = SLACK_MESSAGES[all_reps_found][all_emails_in_all_reps]

    channel = "#events" if all_reps_found else "#urgent"

    slack_client.api_call(
        "chat.postMessage",
        channel=channel,
        text=text.format(mdate=mdate, reps=reps_not_found, emails=not_found_emails_in_all_reps),
        username="Vlad",
        icon_emoji=":drunk_russian:"
    )


def main(mdate=datetime.now()):
    """
        Controls all the workflow of the script
    """

    mdate = mdate.replace(day=1) - timedelta(days=1)

    not_found_emails_in_all_reps = []
    reps_not_found = []

    for rep in REPS:
        evidences, rep_not_found = get_evidences(rep, mdate)
        if rep_not_found:
            reps_not_found.append(rep['name'])

        ev_by_per, not_found_emails = evidences_by_person(evidences)
        not_found_emails_in_all_reps += not_found_emails
        not_found_emails_in_all_reps = list(set(not_found_emails_in_all_reps))

        create_evidences(ev_by_per, mdate, rep['name'])

    for func in [create_zips, upload_to_s3, upload_to_drive]:
        func(mdate)

    send_slack_message(
        not_found_emails_in_all_reps=not_found_emails_in_all_reps,
        mdate=mdate,
        reps_not_found=reps_not_found
    )


# This comment is to disable pylint warnings, because we don't use variables
# event and context, but they have to be in the lambda_handler definition
#pylint: disable=unused-argument
def lambda_handler(event, context):
    """
        Function that is called when the lambda is triggered
    """

    main()
    return True
