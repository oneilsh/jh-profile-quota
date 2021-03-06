from typing import List, Dict, Tuple, Optional, Union
import sqlite3 as sq3
import datetime
import sys

TIME_FMT = "%Y-%m-%d %H:%M:%S"

def get_connection(db_filename: str) -> sq3.Connection:
    conn: sq3.Connection = sq3.connect(db_filename)
    return conn

def close_connection(conn: sq3.Connection) -> None:
    conn.commit()
    conn.close()

# returns the profiles list with an extra "disabled" = True or False in each profile dictionary, determined by 
# the quota metadata in the profiles (minBalanceToSpawn, default to 0.0 if not specified) and the users' balances
# also an entry for the current "balanceTokens" and "balanceHours" (the latter computed according to the balance in tokens and the
# profiles cost per hour (if the cost per hour is 0, the balanceHours is set as float("inf"), python's infinity)
# for profiles without a quota set, entries are not added
# if the user doesn't have a balance defined, it defaults to 0.0 (thus call update_user_tokens before this to initialize/update balances)
def get_profiles_by_balance(conn: sq3.Connection, profiles: List, user: str, is_admin: bool) -> List:
    ensure_initialized(conn, profiles, user, is_admin) 
    c = conn.cursor()

    return_profiles: List = []

    profile: Dict
    for profile in profiles:
        profile_slug: str = profile["slug"]

        if "quota" in profile:
            min_to_spawn: float = profile["quota"].get("minBalanceToSpawn", 0.0)
            cost_tokens_per_hour: float = profile["quota"].get("costTokensPerHour", 1.0)

            new_tokens_per_day: float = profile["quota"].get("users", {}).get("newTokensPerDay", 0.0)
            if is_admin:
                new_tokens_per_day = profile["quota"].get("admins", {}).get("newTokensPerDay", 0.0)

            max_balance: float = profile["quota"].get("users", {}).get("maxBalance", float("inf"))
            if is_admin:
                max_balance = profile["quota"].get("admins", {}).get("maxBalance", float("inf"))

            is_disabled: bool = profile["quota"].get("users", {}).get("disabled", False)
            if is_admin:
                is_disabled: bool = profile["quota"].get("admins", {}).get("disabled", False)

            c.execute("SELECT count FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
            res: Optional[Tuple[float]] = c.fetchone()
            
            # this is just to shut mypy up - we ensured initialized above
            balance: float = 0.0
            if res:
                balance = res[0]
                
            balance_hours: Union[float, str] = "Infinite"
            new_hours_per_day: float = 0.0
            min_to_spawn_hours: float = 0.0
            max_balance_hours: float = 0.01
            if cost_tokens_per_hour > 0:
                balance_hours = balance / cost_tokens_per_hour
                new_hours_per_day = new_tokens_per_day / cost_tokens_per_hour
                min_to_spawn_hours = min_to_spawn / cost_tokens_per_hour
                max_balance_hours = max_balance / cost_tokens_per_hour

            profile["hasQuota"] = True
            profile["quotaDisplayRateHoursPerDay"] = round(new_hours_per_day, 1)
            if balance <= min_to_spawn:
                profile["quotaDisplayDisabled"] = True
            # round to 1 decimal for display, down for balance and up for minimum to start so users aren't confused about edge cases
            profile["quotaDisplayBalanceTokens"] = int(balance * 10.0)/10        # round decimal (floor)
            profile["quotaDisplayBalanceHours"] = int(balance_hours * 10.0)/10   # floor
            if max_balance == float("inf"):
                profile["quotaDisplayMaxBalance"] = "Unlimited"
                profile["quotaDisplayMaxBalanceHours"] = "Unlimited"
            else:
                profile["quotaDisplayMaxBalance"] = int(max_balance * 10.0)/10              # ceiling
                profile["quotaDisplayMaxBalanceHours"] = int(max_balance_hours * 10.0)/10   # ceiling
            profile["quotaDisplayMinToStartHours"] = int(min_to_spawn_hours * 10 + 0.99)/10 # ceiling
            profile["quotaDisplayDisabled"] = is_disabled

        # allow for disabled: flag in profile to turn profiles off altogether
        if not profile.get("disabled", False) and not profile.get("quotaDisplayDisabled", False):
            return_profiles.append(profile)

    # for use with not-latest jupyterhubs which require an index in the profile_form_template rather than slug
    for i in range(0, len(return_profiles)):
        return_profiles[i]["index"] = i

    return return_profiles


# returns token count; updating their count based on time elapsed since last update
# if user is not defined (e.g. if this is the first time they've logged in, or the first time since the db was wiped...), 
# then returns the initial token count defined in the tokens table
def update_user_tokens(conn: sq3.Connection, profiles: List, user: str, is_admin: bool) -> None: 
    ensure_initialized(conn, profiles, user, is_admin)

    c = conn.cursor()

    profile: Dict
    for profile in profiles:
        profile_slug: str = profile["slug"]
        sys.stderr.write("Checking profile " + profile_slug + "\n")

        rate: float = 0.0
        initial: float = float("inf")
        max_count: float = float("inf")
        active: bool = True

        if "quota" in profile:
            if is_admin:
                rate = profile["quota"].get("admins", {}).get("newTokensPerDay", 0.0)
                initial = profile["quota"].get("admins", {}).get("initialBalance", float("inf"))
                max_count = profile["quota"].get("admins", {}).get("maxBalance", float("inf"))
                active = profile["quota"].get("admins", {}).get("active", True)  # quotas default to active if not specifid
            else:
                rate = profile["quota"].get("users", {}).get("newTokensPerDay", 0.0)
                initial = profile["quota"].get("users", {}).get("initialBalance", float("inf"))
                max_count = profile["quota"].get("users", {}).get("maxBalance", float("inf"))
                active = profile["quota"].get("users", {}).get("active", True)  # quotas default to active if not specifid

        # if the quota isn't active, don't do anything
        if not active:
            continue
         
        nowtime: datetime.datetime = datetime.datetime.now()
        nowtimestamp: str = nowtime.strftime(TIME_FMT)
        
        c.execute("SELECT count, last_add FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
        count_lastadd: Optional[Tuple[float, str]] = c.fetchone()
        
        # again, just to shut up mypy - ran ensure_initialized above
        balance: float = 0.0
        lastadd: str = nowtimestamp
        if count_lastadd:
            balance = float(count_lastadd[0])
            lastadd = count_lastadd[1]

        since_last_duration: datetime.timedelta = nowtime - datetime.datetime.strptime(lastadd, TIME_FMT)
        since_last_seconds: int = since_last_duration.days * 24 * 60 * 60 + since_last_duration.seconds
        since_last_hours: float = since_last_seconds / (60 * 60)
        
        new_accumulated: float = since_last_hours * rate / 24.0
        balance = min(balance + new_accumulated, max_count)
        
        c.execute("UPDATE usertokens SET count='%s', last_add='%s' WHERE user='%s' AND profile_slug = '%s'"%(balance, nowtimestamp, user, profile_slug))
      

def get_initial(profiles_list: List, profile_slug: str, is_admin: bool) -> float:
    profile: Dict
    initial: float = float("inf")
    for profile in profiles_list:
        if "quota" in profile and profile["slug"] == profile_slug:
            if is_admin:
                initial = profile["quota"].get("admins", {}).get("initialBalance", float("inf"))
            else:
                initial = profile["quota"].get("users", {}).get("initialBalance", float("inf"))
    return initial


def ensure_initialized(conn: sq3.Connection, profiles: List, user: str, is_admin: bool) -> None:
    profile: Dict

    c = conn.cursor()

    for profile in profiles:
        profile_slug = profile["slug"]
        initial: float = get_initial(profiles, profile_slug, is_admin)

        c.execute("SELECT count, last_add FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
        count_lastadd: Optional[Tuple[float, str]] = c.fetchone()

        if not count_lastadd:
            nowtime: datetime.datetime = datetime.datetime.now()
            nowtimestamp: str = nowtime.strftime(TIME_FMT)
            c.execute("INSERT INTO usertokens (user, profile_slug, count, last_add) VALUES ('%s', '%s', '%s', '%s')"%(user, profile_slug, initial, nowtimestamp))


def get_balance(conn: sq3.Connection, profiles: List, user: str, profile_slug: str, is_admin: bool) -> float: 
    ensure_initialized(conn, profiles, user, is_admin)

    c = conn.cursor()
    
    c.execute("SELECT count, last_add FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
    count_lastadd: Optional[Tuple[float, str]] = c.fetchone()

    balance: float = 0.0
    if count_lastadd:
        balance = count_lastadd[0]
    
    return balance

def charge_tokens(conn: sq3.Connection, profiles: List, user: str, profile_slug: str, hours: float, is_admin: bool) -> None:
    ensure_initialized(conn, profiles, user, is_admin)

    c = conn.cursor()

    cost_tokens_per_hour: float = 1.0
    profile: Dict
    for profile in profiles:
        if "quota" in profile and profile["slug"] == profile_slug:
            cost_tokens_per_hour = profile["quota"].get("costTokensPerHour", 1.0)
    
    tokens_charged: float = hours * cost_tokens_per_hour
    new_balance: float = get_balance(conn, profiles, user, profile_slug, is_admin) - tokens_charged
    c.execute("UPDATE usertokens SET count='%s' WHERE user='%s' AND profile_slug='%s';"%(new_balance, user, profile_slug)) 
   

def log_usage(conn: sq3.Connection, profiles: List, user: str, profile_slug: str, hours: float, is_admin: bool) -> None:
    c = conn.cursor()

    timestamp: str = datetime.datetime.now().strftime(TIME_FMT)
    
    cost_tokens_per_hour: float = 1.0
    profile: Dict
    for profile in profiles:
        if "quota" in profile and profile["slug"] == profile_slug:
            cost_tokens_per_hour = profile["quota"].get("costTokensPerHour", 1.0)
    
    tokens: float = hours * cost_tokens_per_hour
    cmd: str = "INSERT INTO usage (user, date, profile_slug, hours, tokens) VALUES ('%s', '%s', '%s', '%s', '%s');"%(user, timestamp, profile_slug, hours, tokens)
    c.execute(cmd)
   

def create_db(filename: str) -> None:
    conn = sq3.connect(filename)
    
    c = conn.cursor()

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

    conn.commit()
    conn.close()
