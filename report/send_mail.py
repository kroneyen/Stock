"""
Adapted from:
https://github.com/google/gmail-oauth2-tools/blob/master/python/oauth2.py
https://developers.google.com/identity/protocols/OAuth2

1. Generate and authorize an OAuth2 (generate_oauth2_token)
2. Generate a new access tokens using a refresh token(refresh_token)
3. Generate an OAuth2 string to use for login (access_token)
"""

import base64
import imaplib
import json
import smtplib
import urllib.parse
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import lxml.html
import redis
import os


pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)


GOOGLE_ACCOUNTS_BASE_URL = 'https://accounts.google.com'
#EDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
REDIRECT_URI ='https://localhost:1'


GOOGLE_CLIENT_ID = r.hget('oauth','GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = r.hget('oauth','GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = r.hget('oauth','GOOGLE_REFRESH_TOKEN') 
#GOOGLE_REFRESH_TOKEN = None

def command_to_url(command):
    return '%s/%s' % (GOOGLE_ACCOUNTS_BASE_URL, command)


def url_escape(text):
    return urllib.parse.quote(text, safe='~-._')


def url_unescape(text):
    return urllib.parse.unquote(text)


def url_format_params(params):
    param_fragments = []
    for param in sorted(params.items(), key=lambda x: x[0]):
        param_fragments.append('%s=%s' % (param[0], url_escape(param[1])))
    return '&'.join(param_fragments)


def generate_permission_url(client_id, scope='https://mail.google.com/'):
    params = {}
    params['client_id'] = client_id
    params['redirect_uri'] = REDIRECT_URI
    params['scope'] = scope
    params['response_type'] = 'code'
    return '%s?%s' % (command_to_url('o/oauth2/auth'), url_format_params(params))


def call_authorize_tokens(client_id, client_secret, authorization_code):
    params = {}
    params['client_id'] = client_id
    params['client_secret'] = client_secret
    params['code'] = authorization_code
    params['redirect_uri'] = REDIRECT_URI
    params['grant_type'] = 'authorization_code'
    request_url = command_to_url('o/oauth2/token')
    response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
    return json.loads(response)


def call_refresh_token(client_id, client_secret, refresh_token):
    params = {}
    params['client_id'] = client_id
    params['client_secret'] = client_secret
    params['refresh_token'] = refresh_token
    params['grant_type'] = 'refresh_token'
    request_url = command_to_url('o/oauth2/token')
    response = urllib.request.urlopen(request_url, urllib.parse.urlencode(params).encode('UTF-8')).read().decode('UTF-8')
    return json.loads(response)


def generate_oauth2_string(username, access_token, as_base64=False):
    auth_string = 'user=%s\1auth=Bearer %s\1\1' % (username, access_token)
    if as_base64:
        auth_string = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
    return auth_string


def test_imap(user, auth_string):
    imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
    imap_conn.debug = 4
    imap_conn.authenticate('XOAUTH2', lambda x: auth_string)
    imap_conn.select('INBOX')


def test_smpt(user, base64_auth_string):
    smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_conn.set_debuglevel(True)
    smtp_conn.ehlo('test')
    smtp_conn.starttls()
    smtp_conn.docmd('AUTH', 'XOAUTH2 ' + base64_auth_string)


def get_authorization(google_client_id, google_client_secret):
    scope = "https://mail.google.com/"
    print('Navigate to the following URL to auth:', generate_permission_url(google_client_id, scope))
    authorization_code = input('Enter verification code: ')
    response = call_authorize_tokens(google_client_id, google_client_secret, authorization_code)
    return response['refresh_token'], response['access_token'], response['expires_in']


def refresh_authorization(google_client_id, google_client_secret, refresh_token):
    response = call_refresh_token(google_client_id, google_client_secret, refresh_token)
    return response['access_token'], response['expires_in']

"""
def send_mail(fromaddr, toaddr, subject, message):
    access_token, expires_in = refresh_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    auth_string = generate_oauth2_string(fromaddr, access_token, as_base64=True)

    msg = MIMEMultipart('related')
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg.preamble = 'This is a multi-part message in MIME format.'
    msg_alternative = MIMEMultipart('alternative')
    msg.attach(msg_alternative)
    part_text = MIMEText(lxml.html.fromstring(message).text_content().encode('utf-8'), 'plain', _charset='utf-8')
    part_html = MIMEText(message.encode('utf-8'), 'html', _charset='utf-8')
    msg_alternative.attach(part_text)
    msg_alternative.attach(part_html)
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo(GOOGLE_CLIENT_ID)
    server.starttls()
    server.docmd('AUTH', 'XOAUTH2 ' + auth_string)
    server.sendmail(fromaddr, toaddr, msg.as_string())
    server.quit()
"""


def send_email(subject, body):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    #import configparser
    #import ast
    import redis
    import os
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    ## get values from config mail section 
    m_from = config.get('mail','m_from')
    m_to = ast.literal_eval(config.get('mail','m_to'))
    m_pwd = config.get('mail','m_pwd')
    """
    ### redis data
    ### parameter redis.StrictRedis(host='localhost', port=6379, db=0, password=None, socket_timeout=None, connection_pool=None, charset='utf-8', errors='strict', decode_responses=False, uni    x_socket_path=None)
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    r = redis.Redis(connection_pool=pool)

    m_from = r.hget('mail','m_from')
    m_to = r.hget('mail','m_to')
    m_pwd = r.hget('mail','m_pwd')

    FROM = m_from
    TO = m_to
    access_token, expires_in = refresh_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
    auth_string = generate_oauth2_string(m_from, access_token, as_base64=True)    

    # Prepare actual message
    #msgText = MIMEText(body,'plain','utf-8')   ## mail body of chinese , setting harset utf8
    msgText = MIMEText(body,'html','utf-8')   ## mail body of chinese , setting harset utf8
    msg = MIMEMultipart()  ## merge mail  multiple part 
    msg['Subject'] = subject
    #msg.attach(msgText)
    attachments = []
    #get folder file for .png
    #items = os.listdir(".")  ## os.listdir (path)  
    items = os.listdir("images")  ## os.listdir (path)  
    for names in items :
      if  names.endswith(".png") : ##find out * '.png' file

          attachments.append("images/"+names)
          #attachments.append(names)

    for file in attachments :
        try:
            with open(file,'rb') as fp :
                 att = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8') ## attachemnt content of utf8
                 att["Content-Type"] = 'application/octet-stream'
                 att["Content-Disposition"] = 'attachment;filename="' + file + '"'  ## attachment with file name
                 msg.attach(att)

        except :
                if __name__ == '__main__':
                  print('attachemnt is failed')


    msg.attach(msgText)
    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo(GOOGLE_CLIENT_ID)
        server.starttls()
        server.docmd('AUTH', 'XOAUTH2 ' + auth_string)
        server.sendmail(m_from, m_to, msg.as_string())
        server.quit()        
        if __name__ == '__main__':
         print ('successfully sent the mail')
    except:
        if __name__ == '__main__':
         print ('failed to send mail')

if __name__ == '__main__':
    if GOOGLE_REFRESH_TOKEN is None:
        print('No refresh token found, obtaining one')
        refresh_token, access_token, expires_in = get_authorization(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)
        print('Set the following as your GOOGLE_REFRESH_TOKEN:', refresh_token)
        print('Set the following as your : expires_in' ,expires_in)
        exit()
    send_email('A mail from you from Python',
              '<b>A mail from you from Python</b><br><br>' +
              'So happy to hear from you so sosooooooooo!')
