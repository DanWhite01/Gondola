import argparse


def str2usercreds(credentials):
    username, pwd, url, db = '', '', '', ''
    try:
        username = credentials.split(':', 1)[0]
        pwd = credentials.split(':', 1)[1].split('@', 1)[0]
        url = credentials.split(':', 1)[1].split('@', 1)[1].rsplit(':', 1)[0]
        db = credentials.split(':', 1)[1].split('@', 1)[1].rsplit(':', 1)[1]

    except IndexError:
        pass

    if any(v == "" for v in [username, pwd, url, db]):
        raise argparse.ArgumentTypeError(
            'Credentials variable not correctly inputted')
    return {'user': username, 'password': pwd, 'account': url, 'database': db}
