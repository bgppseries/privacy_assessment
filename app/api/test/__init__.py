from flask import Blueprint, jsonify
from flask_cors import CORS


api_test = Blueprint('api_test', __name__)

CORS(api_test)

from . import view
