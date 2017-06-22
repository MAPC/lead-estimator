import pandas as pd
from os import path

class Estimator(object):

  def __new__(self, fn):
    def estimator(data_sources):
      data = {}

      file_readers = {
        'csv': pd.read_csv,
        'xls': pd.read_excel,
        'xlsx': pd.read_excel
      }

      for data_source in data_sources:
        file_type = path.splitext(data_source['file_path'])[1][1:]
        data[data_source['tag']] = file_readers[file_type](data_source['file_path'])

      return fn(data)

    return estimator
