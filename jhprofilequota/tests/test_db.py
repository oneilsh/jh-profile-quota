from typing import Optional, Tuple
from unittest import TestCase
import sqlite3 as sq3
import os

import jhprofilequota
from jhprofilequota import db


class TestDb(TestCase):
    def test_add_profile(self) -> None:
        db.create_db("testing.db")
    






