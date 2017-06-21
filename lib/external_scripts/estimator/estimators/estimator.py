import pandas as pd


class Estimator(object):

  def __new__(self, fn):
    def estimator(data_sources):
      data = {}

      for data_source in data_sources:
        data[data_source] = pd.read_excel(data_source)

      return fn(data)

    return estimator
