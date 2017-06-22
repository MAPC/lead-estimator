from .estimator import Estimator

def commercial(data_source):

  def methodology(data):
    return data

  return Estimator(methodology)(data_source)
