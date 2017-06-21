from .estimator import Estimator

def commercial(data_source):

  def processor(data):
    return data

  return Estimator(processor)(data_source)
