# api/index.py

import os
from app import app  # Replace 'your_flask_app' with the name of your main Python file without the .py extension
import awsgi

def handler(request, context):
    return awsgi.response(app, request, context)
