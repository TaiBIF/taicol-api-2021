import json
import pymysql
import pandas as pd
import numpy as np
import datetime
import requests
import os
import time
from typing import List
from json import JSONEncoder

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.views import APIView
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from sqlalchemy import create_engine

from conf.settings import env, SOLR_PREFIX
from api.utils import db_settings

match_url = env('NOMENMATCH_URL')

reference_type_map = {
    1: 'Journal Article',
    2: 'Book Chapter',
    3: 'Book',
    4: 'Backbone',
    5: 'Checklist',
    6: 'Backbone'
}

# type= 1 or 2 or 3 地位是相同的
custom_reference_type_order = {
    1: 2,
    2: 2,
    3: 2,
    4: 4,
    5: 3,
    6: 1
}

bio_group_map = {
    "Insects": "昆蟲",
    "Spiders": "蜘蛛",
    "Fishes": "魚類",
    "Reptiles": "爬蟲類",
    "Amphibians": "兩棲類",
    "Birds": "鳥類",
    "Mammals": "哺乳類",
    "Vascular Plants": "維管束植物",
    "Ferns": "蕨類植物",
    "Mosses": "苔蘚植物",
    "Algae": "藻類",
    "Viruses": "病毒",
    "Bacteria": "細菌",
    "Fungi": "真菌",
}


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
