"""
  Class: Estimator

  Curried class who's first call consumes a methodology function which is used 
  by the second call that loads in and injects datasets into the methodology.
"""

import settings
import sqlalchemy
import pandas as pd
from os import path


class Estimator(object):

  loaded_files = {}

  database_tag_map = {
    'cbecs_el': '',
    'cbecs_fo': '',
    'cbecs_ng': '',
    'cbecs_sources': '',
    'mecs_ami': '',
    'mecs_fce': '',
    'mecs_fci': '',
    'recs_hfc': '',
    'recs_hfe': '',
    'recs_sc': '',
    'acs_uis': '',
    'acs_hf': '',
  }

  postgres_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(settings.db.USER, settings.db.PASSWORD, settings.db.HOST, settings.db.PORT, settings.db.NAME))


  def __new__(self, fn):
    """
      @param Function<[Dict<DataFrame>],DataFrame> fn

      @return Estimator
    """

    def estimator(data_sources):
      """
        @param List<Dict<String>> data_sources

        @return DataFrame
      """

      data = {}

      file_readers = {
        'csv': pd.read_csv,
        'xls': pd.read_excel,
        'xlsx': pd.read_excel
      }

      for data_source in data_sources:
        if not data_source['tag'] in Estimator.loaded_files:
          file_type = path.splitext(data_source['file_path'])[1][1:]
          Estimator.loaded_files[data_source['tag']] = file_readers[file_type](data_source['file_path'])

        data[data_source['tag']] = Estimator.loaded_files[data_source['tag']]



      return fn(data)

    return estimator
