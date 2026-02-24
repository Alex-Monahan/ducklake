"""WSGI entry point for moto S3 server, used by gunicorn.

Creates a single S3 backend app. All requests within the same process share
the same in-memory moto state.
"""
from moto.moto_server.werkzeug_app import create_backend_app

app = create_backend_app("s3")
