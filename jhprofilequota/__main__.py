#!/usr/bin/env python3
"""script to monitor and cull single-user servers by quota

This is a modification of the cull_idle_servers.py script bundled with jupyterhub, originally sourced 
from https://github.com/jupyterhub/jupyterhub/blob/d126baa443ad7d893be2ff4a70afe9ef5b8a4a1a/examples/cull-idle/cull_idle_servers.py
Main differences:
  - no cull_users check
  - no max_age check
  - no inactivity checks or timeout defined
  - check based on balance from db!


You can run this as a service managed by JupyterHub with this in your config::


    c.JupyterHub.services = [
        {
            'name': 'cull-quota',
            'admin': True,
            'command': [sys.executable, '-m', 'jhprofilequota', '--check_every=600', '--db_file=profile_quotas.db'],
        }
    ]

Or run it manually by generating an API token and storing it in `JUPYTERHUB_API_TOKEN`:

    export JUPYTERHUB_API_TOKEN=$(jupyterhub token)
    python3 -m profilequota [--check_every=600] [--url=http://127.0.0.1:8081/hub/api] --db_file=profile_quotas.db

"""
import json
import os
from datetime import datetime
from datetime import timezone
from functools import partial

try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote # type: ignore

import dateutil.parser

from tornado.gen import coroutine, multi # type: ignore
from tornado.locks import Semaphore
from tornado.log import app_log
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.options import define, options, parse_command_line

import db

def parse_date(date_string):
    """Parse a timestamp

    If it doesn't have a timezone, assume utc

    Returned datetime object will always be timezone-aware
    """
    dt = dateutil.parser.parse(date_string)
    if not dt.tzinfo:
        # assume naive timestamps are UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def format_td(td):
    """
    Nicely format a timedelta object

    as HH:MM:SS
    """
    if td is None:
        return "unknown"
    if isinstance(td, str):
        return td
    seconds = int(td.total_seconds())
    h = seconds // 3600
    seconds = seconds % 3600
    m = seconds // 60
    seconds = seconds % 60
    return "{h:02}:{m:02}:{seconds:02}".format(h=h, m=m, seconds=seconds)


@coroutine
def cull_idle(
    url, api_token, profiles_list = [], db_filename = "profile_quotas.db", check_every = 600, concurrency=10
):
    """Shutdown idle single-user servers"""
    
    auth_header = {'Authorization': 'token %s' % api_token}
    req = HTTPRequest(url=url + '/users', headers=auth_header)
    now = datetime.now(timezone.utc)
    client = AsyncHTTPClient()

    if concurrency:
        semaphore = Semaphore(concurrency)

        @coroutine
        def fetch(req):
            """client.fetch wrapped in a semaphore to limit concurrency"""
            yield semaphore.acquire()
            try:
                return (yield client.fetch(req))
            finally:
                yield semaphore.release()

    else:
        fetch = client.fetch

    resp = yield fetch(req)
    users = json.loads(resp.body.decode('utf8', 'replace'))
    futures = []

    @coroutine
    def handle_server(user, server_name, server):
        """Handle (maybe) culling a single server

        "server" is the entire server model from the API.

        Returns True if server is now stopped (user removable),
        False otherwise.
        """
        log_name = user['name']
        if server_name:
            log_name = '%s/%s' % (user['name'], server_name)
        if server.get('pending'):
            app_log.warning(
                "Not culling server %s with pending %s", log_name, server['pending']
            )
            return False

        # jupyterhub < 0.9 defined 'server.url' once the server was ready
        # as an *implicit* signal that the server was ready.
        # 0.9 adds a dedicated, explicit 'ready' field.
        # By current (0.9) definitions, servers that have no pending
        # events and are not ready shouldn't be in the model,
        # but let's check just to be safe.

        if not server.get('ready', bool(server['url'])):
            app_log.warning(
                "Not culling not-ready not-pending server %s: %s", log_name, server
            )
            return False

        if server.get('started'):
            age = now - parse_date(server['started'])
        else:
            # started may be undefined on jupyterhub < 0.9
            age = None


        # CUSTOM CULLING TEST CODE HERE
        # Add in additional server tests here.  Return False to mean "don't
        # cull", True means "cull immediately", or, for example, update some
        # other variables like inactive_limit.
        #
        # Here, server['state'] is the result of the get_state method
        # on the spawner.  This does *not* contain the below by
        # default, you may have to modify your spawner to make this
        # work.  The `user` variable is the user model from the API.
        #
        # if server['state']['profile_name'] == 'unlimited'
        #     return False
        # inactive_limit = server['state']['culltime']

        should_cull = False

        # if there's no profile info in the server state to base the determinaton on, we got nothing to go on
        profile_slug = server.get("state", {}).get("profile_name", None)
        balance = float("inf")

        if profile_slug:
            db.update_user_tokens(db_filename, profiles_list, user['name'], user['admin'])
            
            for profile in profiles_list:
                if profile["slug"] == profile_slug and "quota" in profile:
                    hours = (check_every / 60 / 60)
                    db.log_usage(db_filename, profiles_list, user['name'], profile_slug, hours, user['admin'])
                    db.charge_tokens(db_filename, profiles_list, user['name'], profile_slug, hours, user['admin']) # TODO
                    current_balance = db.get_balance(db_filename, profiles_list, user['name'], profiles_slug, user['admin']) # TODO

                    if current_balance < 0.0:
                        should_cull = True

        if should_cull:
            app_log.info(
                "Culling server %s (balance for profile %s is %s)", log_name, profile_slug, balance
            )

        if not should_cull:
            app_log.debug(
                "Not culling server %s (balance for profile %s is %s)",
                log_name,
                profile_slug,
                balance,
            )
            return False

        if server_name:
            # culling a named server
            delete_url = url + "/users/%s/servers/%s" % (
                quote(user['name']),
                quote(server['name']),
            )
        else:
            delete_url = url + '/users/%s/server' % quote(user['name'])

        req = HTTPRequest(url=delete_url, method='DELETE', headers=auth_header)
        resp = yield fetch(req)
        if resp.code == 202:
            app_log.warning("Server %s is slow to stop", log_name)
            # return False to prevent culling user with pending shutdowns
            return False
        return True

    @coroutine
    def handle_user(user):
        """Handle one user.

        Create a list of their servers, and async exec them.  Wait for
        that to be done, and if all servers are stopped, possibly cull
        the user.
        """
        # shutdown servers first.
        # Hub doesn't allow deleting users with running servers.
        # jupyterhub 0.9 always provides a 'servers' model.
        # 0.8 only does this when named servers are enabled.
        if 'servers' in user:
            servers = user['servers']
        else:
            # jupyterhub < 0.9 without named servers enabled.
            # create servers dict with one entry for the default server
            # from the user model.
            # only if the server is running.
            servers = {}
            if user['server']:
                servers[''] = {
                    'last_activity': user['last_activity'],
                    'pending': user['pending'],
                    'url': user['server'],
                }
        server_futures = [
            handle_server(user, server_name, server)
            for server_name, server in servers.items()
        ]
        results = yield multi(server_futures)
    
    
    
    
    for user in users:
        futures.append((user['name'], handle_user(user)))

    for (name, f) in futures:
        try:
            result = yield f
        except Exception:
            app_log.exception("Error processing %s", name)
        else:
            if result:
                app_log.debug("Finished culling %s", name)


if __name__ == '__main__':
    define(
        'profiles_json',
        default=os.environ.get('JUPYTERHUB_PROFILES_JSON', '[]'),
        help="Hub profiles as JSON, for use in quota determination (which are stored with profiles)."
    )
    define(
        'url',
        default=os.environ.get('JUPYTERHUB_API_URL'),
        help="The JupyterHub API URL",
    )
    define(
        'check_every',
        default=600,
        help="The interval (in seconds) for checking for idle servers to cull",
    )
    define(
        'concurrency',
        default=10,
        help="""Limit the number of concurrent requests made to the Hub.

                Deleting a lot of users at the same time can slow down the Hub,
                so limit the number of API requests we have outstanding at any given time.
                """,
    )
    define(
        'quota_db_filename',
        default = 'profile_quotas.db',
        help="File path for sqlite3 database to use for quota storage; will be created if it doesn't exist.",
    )

    parse_command_line()
    if not options.check_every:
        options.check_every = 600
    api_token = os.environ['JUPYTERHUB_API_TOKEN']

    profiles_list = json.loads(options.profiles_json)

    try:
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
    except ImportError as e:
        app_log.warning(
            "Could not load pycurl: %s\n"
            "pycurl is recommended if you have a large number of users.",
            e,
        )

    loop = IOLoop.current()
    cull = partial(
        cull_idle,
        url=options.url,
        api_token=api_token,
        profiles_list=profiles_list,
        db_filename=options.quota_db_filename,
        check_every=options.check_every,
        concurrency=options.concurrency,
    )
    # schedule first cull immediately
    # because PeriodicCallback doesn't start until the end of the first interval
    loop.add_callback(cull)
    # schedule periodic cull
    pc = PeriodicCallback(cull, 1e3 * options.check_every)
    pc.start()
    try:
        loop.start()
    except KeyboardInterrupt:
        pass
