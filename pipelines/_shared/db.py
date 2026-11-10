"""Tiny shared DB helper for scrapers / scripts."""
import os

import psycopg2


def get_connection():
    """Return a new psycopg2 connection using DATABASE_URL from the env."""
    return psycopg2.connect(os.environ["DATABASE_URL"])
