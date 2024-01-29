from flask import Blueprint, jsonify
from flask_cors import CORS


api_file = Blueprint('api_file', __name__)

CORS(api_file)

from . import view,module
