"""
  Class: Estimator

  Curried class who's first call consumes a methodology function which is used 
  by the second call that loads in and injects datasets into the methodology.
"""

#import settings
import sqlalchemy
import pandas as pd
from os import path
from pprint import pprint


class Estimator(object):

  loaded_data = {}

  database_tag_map = {
      """
    'eowld': 'econ_es202_naics_3d_m',
    'cbecs_elec': 'energy_cbecs_elec_consumption_expenditure_us',
    'cbecs_foil': 'energy_cbecs_fueloil_consumption_expenditure_us',
    'cbecs_ng': 'energy_cbecs_natgas_consumption_expenditure_us',
    'cbecs_sources': '',
    'mecs_ami': 'energy_mecs_fuel_consumption_ne_us',
    'mecs_fce': 'energy_mecs_consumption_ratios_us_us',
    'recs_hfc': 'energy_recs_hh_fuel_consumption_ne_us',
    'recs_hfe': 'energy_recs_hh_fuel_expenditures_ne_us',
    'recs_sc': 'energy_recs_hu_structural_characteristics',
    'acs_uis': 'b25024_hu_units_in_structure_acs_m',
    'acs_hf': 'b25117_hu_tenure_by_fuel_acs_m',
    """
  }

  #postgres_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(settings.db.USER, settings.db.PASSWORD, settings.db.HOST, settings.db.PORT, settings.db.NAME))


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
        if not data_source['tag'] in Estimator.loaded_data:
          file_type = path.splitext(data_source['file_path'])[1][1:]
          Estimator.loaded_data[data_source['tag']] = file_readers[file_type](data_source['file_path'])

        data[data_source['tag']] = Estimator.loaded_data[data_source['tag']]


      """
      for tag, table in Estimator.database_tag_map.items():
        if not tag in Estimator.loaded_data:
          data[tag] = pd.read_sql_query("SELECT * FROM tabular." + table, engine=Estimator.postgres_engine)
      """


      return fn(data)

    return estimator
