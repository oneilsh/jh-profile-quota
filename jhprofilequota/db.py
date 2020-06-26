from typing import List, Dict, Tuple, Optional, Union
import sqlite3 as sq3
import datetime
import sys

TIME_FMT = "%Y-%m-%d %H:%M:%S"

# returns the profiles list with an extra "disabled" = True or False in each profile dictionary, determined by 
# the quota metadata in the profiles (minBalanceToSpawn, default to 0.0 if not specified) and the users' balances
# also an entry for the current "balanceTokens" and "balanceHours" (the latter computed according to the balance in tokens and the
# profiles cost per hour (if the cost per hour is 0, the balanceHours is set as "Unlimited")
# for profiles without a quota set, entries are not added

# if the user doesn't have a balance defined, it defaults to 0.0 (thus call update_user_tokens before this to initialize/update balances)
def get_profiles_by_balance(db_filename: str, user: str, profiles: List, is_admin: bool) -> List:
    conn: sq3.Connection = sq3.connect(db_filename)
    c = conn.cursor()

    profile: Dict
    for profile in profiles:
        profile_slug: str = profile["slug"]

        if "quota" in profile:
            min_to_spawn: float = profile["quota"].get("minBalanceToSpawn", 0.0)
            cost_tokens_per_hour: float = profile["quota"].get("costTokensPerHour", 0.0)

            c.execute("SELECT count FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
            res: Optional[Tuple[float]] = c.fetchone()
            
            balance: float
            if res:
                balance = res[0]
            else:
                balance = 0.0

            balance_hours: Union[float, str] = "Unlimited"
            if cost_tokens_per_hour > 0:
                balance_hours = balance / cost_tokens_per_hour

            profile["disabled"] = balance < min_to_spawn
            profile["balanceTokens"] = balance
            profile["balanceHours"] = balance_hours


    conn.commit()
    conn.close()

    return profiles



# returns token count; updating their count based on time elapsed since last update
# if user is not defined (e.g. if this is the first time they've logged in, or the first time since the db was wiped...), 
# then returns the initial token count defined in the tokens table
def update_user_tokens(db_filename: str, user: str, profiles: List, is_admin: bool) -> None: 
    profile: Dict

    conn: sq3.Connection = sq3.connect(db_filename)
    c = conn.cursor()

    for profile in profiles:
        profile_slug: str = profile["slug"]
        sys.stderr.write("Checking profile " + profile_slug + "\n")

        rate: float = 0.0
        initial: float = 0.0
        max_count: float = 0.0
        active: bool = True

        if "quota" in profile:
            if is_admin:
                rate = profile["quota"].get("admins", {}).get("newTokensPerHour", 0.0)
                initial = profile["quota"].get("admins", {}).get("initialBalance", 0.0)
                max_count = profile["quota"].get("admins", {}).get("maxBalance", 0.0)
                active = profile["quota"].get("admins", {}).get("active", True)  # quotas default to active if not specifid
                sys.stderr.write("  initial is " + str(initial) + "for admins...\n")
            else:
                rate = profile["quota"].get("users", {}).get("newTokensPerHour", 0.0)
                initial = profile["quota"].get("users", {}).get("initialBalance", 0.0)
                max_count = profile["quota"].get("users", {}).get("maxBalance", 0.0)
                active = profile["quota"].get("users", {}).get("active", True)  # quotas default to active if not specifid

        # if the quota isn't active, don't do anything
        if not active:
            continue
         
        nowtime: datetime.datetime = datetime.datetime.now()
        nowtimestamp: str = nowtime.strftime(TIME_FMT)
        
        c.execute("SELECT count, last_add FROM usertokens WHERE user='%s' AND profile_slug='%s';"%(user, profile_slug))
        count_lastadd: Optional[Tuple[float, str]] = c.fetchone()
        balance: float = 0.0
    
        # if they have a balance in the db, we need to update it
        if count_lastadd:
            balance = count_lastadd[0]
            lastadd: str = count_lastadd[1]
            since_last_duration: datetime.timedelta = nowtime - datetime.datetime.strptime(lastadd, TIME_FMT)
            since_last_seconds: int = since_last_duration.days * 24 * 60 * 60 + since_last_duration.seconds
            since_last_hours: float = since_last_seconds / (60 * 60)
            new_accumulated: float = since_last_hours * rate
            balance = min(balance + new_accumulated, max_count)
            c.execute("UPDATE usertokens SET count='%s', last_add='%s' WHERE user='%s' AND profile_slug = '%s'"%(balance, nowtimestamp, user, profile_slug))
        # if they don't have a balance in the db, we need to create a new balance
        else:
            balance = initial
            sys.stderr.write(" setting balance to " + str(balance) + "for admins...\n")
            c.execute("INSERT INTO usertokens (user, profile_slug, count, last_add) VALUES ('%s', '%s', '%s', '%s')"%(user, profile_slug, balance, nowtimestamp))
    
    conn.commit()
    conn.close()
    

# logs usage
def log_usage(db_filename: str, profiles: List, user: str, profile_slug: str, hours: float, is_admin: bool) -> None:
    conn: sq3.Connection = sq3.connect(db_filename)
    c = conn.cursor()

    timestamp: str = datetime.datetime.now().strftime(TIME_FMT)
    
    cost_tokens_per_hour: float = 0.0
    profile: Dict
    for profile in profiles:
        if "quota" in profile and profile["slug"] == profile_slug:
            cost_tokens_per_hour = profile["quota"].get("costTokensPerHour", 0.0)
    
    tokens: float = hours * cost_tokens_per_hour
    cmd: str = "INSERT INTO usage (user, date, profile_slug, hours, tokens) VALUES ('%s', '%s', '%s', '%s', '%s');"%(user, timestamp, profile_slug, hours, tokens)
    c.execute(cmd)
   
    conn.commit()
    conn.close()

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
