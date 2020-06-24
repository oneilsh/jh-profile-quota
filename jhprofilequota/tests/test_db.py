from typing import Optional, Tuple
from unittest import TestCase
import sqlite3 as sq3
import os

import jhprofilequota
from jhprofilequota import db

class TestHi(TestCase):
    def test_sayhi(self) -> None:
        s = jhprofilequota.sayhi()
        self.assertTrue(s == "hi")

class TestDb(TestCase):
    def test_add_profile(self) -> None:
        conn: sq3.Connection = db.create_db("testing.db")
        db.add_profile(conn, "standard", "Standard", True, "oneilsh/ktesting_tensorflow_notebook:v1.0.1", 
                       0.1, 1.0, 0.5, 1.0, 0, True, True, 
                       1.0, 0.2, 0.1, 15, 10, 25, 20)
        
        res: float = db.update_user_token(conn, "oneils", "standard", is_admin = True, adjust = 0.0)
        db.add_usage(conn, "oneils", "standard", hours = 0.1, is_admin = True)
    






