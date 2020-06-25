from typing import List, Dict, Tuple, Optional
import sqlite3 as sq3
import datetime

TIME_FMT = "%Y-%m-%d %H:%M:%S"

# returns token count; updating their count based on time elapsed since last update
# if user is not defined (e.g. if this is the first time they've logged in, or the first time since the db was wiped...), 
# then returns the initial token count defined in the tokens table
def update_user_token(conn: sq3.Connection, user: str, profile_slug: str, is_admin: bool, adjust: float = 0.0) -> float:
    c = conn.cursor()
    nowtime: datetime.datetime = datetime.datetime.now()
    nowtimestamp: str = nowtime.strftime(TIME_FMT)
   
    if is_admin:
        c.execute("SELECT rate_admin, initial_admin, max_admin FROM profiles WHERE profile_slug='%s'"%(profile_slug))
    else:
        c.execute("SELECT rate_user, initial_user, max_user FROM profiles WHERE profile_slug='%s'"%(profile_slug))

    res: Optional[Tuple[float, float, float]] = c.fetchone()
    if not res:
        raise IOError("Profile with profile_slug '%s' not found in database, cannot update user token."%(profile_slug))

    rate, initial, max_count = res

    c.execute("SELECT count, last_add FROM usertokens WHERE user='%s';"%(user))
    count_lastadd: Optional[Tuple[float, str]] = c.fetchone()
    balance: float = 0.0
    
    if count_lastadd:
        balance = count_lastadd[0]
        lastadd: str = count_lastadd[1]
        since_last_duration: datetime.timedelta = nowtime - datetime.datetime.strptime(lastadd, TIME_FMT)
        since_last_seconds: int = since_last_duration.days * 24 * 60 * 60 + since_last_duration.seconds
        since_last_hours: float = since_last_seconds / (60 * 60)
        new_accumulated: float = since_last_hours * rate
        balance = min(balance + new_accumulated + adjust, max_count)
        c.execute("UPDATE usertokens SET count='%s', last_add='%s' WHERE user='%s' AND profile_slug = '%s'"%(balance, nowtimestamp, user, profile_slug))
    else:
        balance = min(initial + adjust, max_count)
        c.execute("INSERT INTO usertokens (user, count, last_add, profile_slug) VALUES ('%s', '%s', '%s', '%s')"%(user, balance, nowtimestamp, profile_slug))

    conn.commit()
    return balance

# logs usage and updates token bucket
def add_usage(conn: sq3.Connection, user: str, profile_slug: str, hours: float, is_admin: bool) -> None:
    c = conn.cursor()
    timestamp: str = datetime.datetime.now().strftime(TIME_FMT)
    # TODO: add hours and tokens used
    c.execute("SELECT cost_tokens_per_hour FROM profiles WHERE profile_slug='%s';"%(profile_slug))
    cost_per_hour: float = c.fetchone()[0]
    tokens: float = hours * cost_per_hour
    cmd: str = "INSERT INTO usage (user, date, profile_slug, hours, tokens) VALUES ('%s', '%s', '%s', '%s', '%s');"%(user, timestamp, profile_slug, hours, tokens)
    c.execute(cmd)
    conn.commit()

    update_user_token(conn, user, profile_slug, is_admin, adjust = -1 * tokens)

# to be passed a list as from KubeSpawner.profile_list; many items may need to be null as only display_name and slug are required there
def add_profiles_from_list(conn: sq3.Connection, profile_list: List) -> None:
    c = conn.cursor()
    profile: Dict
    for profile in profile_list:
        profile_slug: str = profile["slug"]
        c.execute("INSERT INTO profiles (profile_slug) VALUES ('%s');"%(profile_slug))
        if "quota" in profile:
            for key in ["active_for_admins", "active_for_users", "initial_admin"]:
                if key in profile["quota"]:
                    value = profile["quota"][key]
                    c.execute("UPDATE profiles SET '%s' = '%s' WHERE profile_slug = '%s';"%(key, value, profile_slug))

    conn.commit()

def create_db(filename: str) -> sq3.Connection:
    conn = sq3.connect(filename)
    
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS profiles (
              profile_slug TEXT PRIMARY KEY,
              active_for_admins TEXT DEFAULT "True",
              active_for_users TEXT DEFAULT "True",
              cost_tokens_per_hour REAL DEFAULT 0.0,
              rate_admin REAL DEFAULT 0.0,
              rate_user REAL DEFAULT 0.0,
              initial_admin REAL DEFAULT 1.0,
              initial_user REAL DEFAULT 1.0,
              max_admin REAL DEFAULT 1.0,
              max_user REAL DEFAULT 1.0
              );''')

    c.execute('''CREATE TABLE IF NOT EXISTS usage (
              user TEXT NOT NULL,
              date TEXT NOT NULL,
              profile_slug TEXT NOT NULL,
              hours TEXT NOT NULL,
              tokens TEXT NOT NULL
              );
              ''')

    c.execute('''CREATE TABLE IF NOT EXISTS usertokens (
              user TEXT NOT NULL,
              profile_slug TEXT NOT NULL,
              count REAL NOT NULL,
              last_add TEXT NOT NULL
              );''')


    c.execute('''CREATE INDEX IF NOT EXISTS idx_usertokens_user ON usertokens(user);''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_usage_user ON usage(user);''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_usage_profile_slug ON usage(profile_slug);''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_usertokens_profile_slug ON usertokens(profile_slug);''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_profiles_profile_slug ON profiles(profile_slug);''')

    conn.commit()
    return conn
