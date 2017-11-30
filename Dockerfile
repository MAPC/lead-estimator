FROM python:3.6

RUN set -ex; \
    \
    mkdir -p /usr/src/app 

VOLUME /usr/src/app
WORKDIR /usr/src/app

RUN set -ex; \
    \
    pip install \
      numpy \
      munch \
      pandas \
      psycopg2 \
      sqlalchemy \
      python-dotenv
