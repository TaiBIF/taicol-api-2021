#!/bin/bash

cd /code


python manage.py collectstatic --no-input

gunicorn --bind 0.0.0.0:8002 --workers=2 --timeout 60 conf.wsgi:application
