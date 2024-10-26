import os
import json
import pandas as pd
import ast
import psutil
import requests

from flask import Flask, request, jsonify
from db_utils import DB
from ace_logger import Logging
from app import app
#
from .BusinessRules import BusinessRules
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string

logging = Logging(name='business_rules_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}
