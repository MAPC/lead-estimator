from os import environ
from munch import Munch
from dotenv import find_dotenv, load_dotenv
load_dotenv(find_dotenv())

db = Munch({
  'HOST': environ.get('DB_HOST'),
  'PORT': environ.get('DB_PORT'),
  'NAME': environ.get('DB_NAME'),
  'USER': environ.get('DB_USER'),
  'PASSWORD': environ.get('DB_PASSWORD'),
})

settings = Munch({
 'db': db
})
