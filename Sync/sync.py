#!/usr/bin/env python
# -*- coding: utf-8 -*-

import acapi
import subprocess
import sys
import re
import logging
from requests.exceptions import HTTPError
from datetime import datetime

class AcquiaWorker(object):
    """Worker class for Acquia"""

    def __init__(self, client):
        super(AcquiaWorker, self).__init__()
        self.client = client

    def acquia_get_environment_details(self, sub: str, env: str) -> dict:
        """Get details related with given Acquia environment
        :param client: Acquia client object.
        :param sub: Acquia subscription name.
        :param env: Acquia environment name.
        :return: A dictionary with Acquia environemnt details.
        """

        dataobj = self.client.site(sub).environment(env)
        data = {}
        data['name'] = str(dataobj['name'])
        data['code_branch'] = str(dataobj['vcs_path'])
        data['ssh_host'] = str(dataobj['ssh_host'])
        data['db_clusters'] = str(dataobj['db_clusters'])
        data['default_domain'] = str(dataobj['default_domain'])
        data['livedev'] = str(dataobj['livedev'])
        return data


class DrushWorker(object):
    """Worker class for Drush"""

    def __init__(self, ssh_url):
        super(DrushWorker, self).__init__()
        self.ssh_url = ssh_url

    def find_drupal_version(self, sub: str) -> str:
        """Finds drupal version for given subscription.
        :param sub: Acquia subscription name.
        :return str: Drupal version.
        """
        proc = subprocess.Popen(['ssh', self.ssh_url, f'cd /mnt/www/html/{sub}.dev/docroot/;',
                                 f'drush status --fields=drupal-version;'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        output, error = proc.communicate()
        if error:
            logging.error('Drupal version not found.')
            exit()

        return re.search(r'\d.+', output.decode('utf-8')).group()

    def drush_command(self, sub: str, env: str, cmd: str) -> None:
        """Run drush command on Acquia server
        :param sub: Acquia subscription name.
        :param env: Acquia environment name.
        :param cmd: Drush command
        """
        d8docroot = f'/mnt/www/html/{sub}.{env}/app/'
        d7docroot = f'/mnt/www/html/{sub}.{env}/docroot/'

        subprocess.check_call(['ssh', self.ssh_url, f'if [ -d {d8docroot} ]; then cd {d8docroot}; \
                                   else cd {d7docroot};  fi;',
                               f'drush {cmd};'])

    def drush_commands_list(self, sub: str, env: str, cmds: list) -> None:
        """Run multiple drush command on Acquia server
        :param sub: Acquia subscription name.
        :param env: Acquia environment name.
        :param cmds: Drush commands.
        """
        [self.drush_command(sub, env, cmd) for cmd in cmds]

    def files_backup(self, sub: str, env: str) -> None:
        """Take Files backup and stores in custombackups folder
        :param ssh_url: SSH url to login into server
        :param sub: Acquia subscription name.
        :param env: Acquia environment name.
        """
        filename = '{sub}.{env}-files-{date}.zip'.format(
            sub=sub, env=env, date=datetime.strftime(datetime.now(), "%Y%m%d%H%M"))
        subprocess.check_call(
            ['ssh', self.ssh_url, f'cd /mnt/gfs/{sub}.{env}/sites/default/files;\
            zip -r /mnt/gfs/{sub}.{env}/custombackups/{filename} *; exit;'])
        logging.info(
            f'{sub}.{env} Files Backup stored in custombackups folder.')


def main():
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    if len(sys.argv) is not 4:
        logging.error(
            'Below arguments are missing: \n Usage: \'python sync.py <sub> <source> <destination>\'')
        quit()

    sub, source, dest = sys.argv[1], sys.argv[2], sys.argv[3]

    if (dest == 'prod'):
        choice = input('Are you sure you want to sync with profuction environment? (y/n)')
        if choice not in ('y','Y'):
            quit()

    sync_choice = input(
        f'\nSelect among following options: \n \n \
        1. Database sync from {source} to {dest} \n \
        2. Files sync from {source} to {dest} \n \
        3. Database and Files sync from {source} to {dest} \n\n')
    if sync_choice not in ('1', '2', '3'):
        logging.error(f'Please select among 1, 2 or 3.')
        quit()

    client = acapi.Client()
    acquia = AcquiaWorker(client)

    logging.info('Validating Subscription and Environment names....')
    try:
        # ssh_url_source = sub + '.' + source + '@' + \
        #    acquia.acquia_get_environment_details(sub, source)['ssh_host']
        ssh_url_dest = sub + '.' + dest + '@' + \
            acquia.acquia_get_environment_details(sub, dest)['ssh_host']
    except HTTPError:
        logging.error(f'Subscription or Environment name is not correct.')
        quit()

    # drush_source = DrushWorker(ssh_url_source)
    drush_dest = DrushWorker(ssh_url_dest)

    drupal_version = drush_dest.find_drupal_version(sub)
    #drupal_version = '7'
    subprocess.check_call(
        ['ssh', drush_dest.ssh_url, f'mkdir -p /mnt/gfs/{sub}.{dest}/custombackups'])

    if sync_choice is '3':
        sync_both = True
    else:
        sync_both = False
        
    if (sync_choice is '1') or sync_both:

        logging.info(f'Taking Database backups of {dest}(Destination)...')
        filename = '{sub}.{dest}-{date}.sql'.format(
            sub=sub, dest=dest, date=datetime.strftime(datetime.now(), "%Y%m%d%H%M"))
        command = f'sql-dump --result-file=/mnt/gfs/{sub}.{dest}/custombackups/{filename}'
        drush_dest.drush_command(
            sub, dest, command)

        DB_prod_sync_choice = input(
            f'\nSelect among following options for {dest}:\
            \n\n1.Database sanitization.\
            \n2.Full DB Sync.\n\n')

        if DB_prod_sync_choice not in ('1', '2'):
            logging.error('Please select among 1 or 2.')
            quit()

        if DB_prod_sync_choice is '1':

            #DB sync
            client.site(sub).environment(source).db(sub).copy(dest)
            logging.info(
                f'Database Sync from {source} to {dest} is completed.\n')

            if drupal_version.startswith('7'):
                commands = ['sql-sanitize -y',
                            'rr --fire-bazooka']
            if drupal_version.startswith('8'):
                commands = ['sql-sanitize -y', 'cr']

            drush_dest.drush_commands_list(
                sub, dest, commands)


        if DB_prod_sync_choice is '2':
            #DB sync
            client.site(sub).environment(source).db(sub).copy(dest)
            logging.info(
                f'Database Sync from {source} to {dest} is completed.\n')
            if drupal_version.startswith('7'):
                commands = ['rr --fire-bazooka']
            if drupal_version.startswith('8'):
                commands = ['cr']
            drush_dest.drush_commands_list(
                sub, dest, commands)

    if (sync_choice is '2') or sync_both:
        drush_dest.files_backup(sub, dest)
        client.site(sub).environment(source).copy_files(dest)
        logging.info(f'Files Sync from {source} to {dest} is completed.\n')
        if drupal_version.startswith('7'):
            drush_dest.drush_command(sub, dest, 'rr --fire-bazooka')
        if drupal_version.startswith('8'):
            drush_dest.drush_command(sub, dest, 'cr')


if __name__ == "__main__":
    main()