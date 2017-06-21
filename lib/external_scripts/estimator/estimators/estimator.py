import pandas as pd


class Estimator(object):

  def __new__(self, fn):
    def estimator(data_source):
      data = pd.read_excel(data_source)

      return fn(data)

    return estimator
