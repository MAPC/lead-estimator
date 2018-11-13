"""
  Class: Estimator

  Curried class who's first call consumes a methodology function which is used 
  by the second call that loads in and injects datasets into the methodology.
"""

from .settings import settings
from .blacklist import blacklist
import sqlalchemy
import pandas as pd
from os import path


# Turn the blacklist items into their lowercase counterparts.
# This is better for normalized comparison since you may not
# know the casing of each item.
lowercase_blacklist = [x.lower() for x in blacklist]


class Estimator(object):

  loaded_data = {}

  database_tag_map = {
    'eowld': 'econ_es202_naics_3d_m',
    'cbecs_elec': 'energy_cbecs_elec_consumption_expenditure_us',
    'cbecs_foil': 'energy_cbecs_fueloil_consumption_expenditure_us',
    'cbecs_ng': 'energy_cbecs_natgas_consumption_expenditure_us',
    'cbecs_sources': 'energy_cbecs_building_energy_sources_us',
    'mecs_ami': 'energy_mecs_fuel_consumption_ne_us',
    'mecs_fce': 'energy_mecs_consumption_ratios_ne_us',
    'recs_hfc': 'energy_recs_hh_fuel_consumption_ne_us',
    'recs_hfe': 'energy_recs_hh_fuel_expenditures_ne_us',
    'recs_sc': 'energy_recs_hu_structural_characteristics',
    'acs_uis': 'b25024_hu_units_in_structure_acs_m',
    'acs_hf': 'b25117_hu_tenure_by_fuel_acs_m',
  }

  db_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(settings.db.USER, settings.db.PASSWORD, settings.db.HOST, settings.db.PORT, settings.db.NAME))


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
          print("Loading " + data_source['tag'] + " from file")
          file_type = path.splitext(data_source['file_path'])[1][1:]
          df = file_readers[file_type](data_source['file_path'])
          Estimator.loaded_data[data_source['tag']] = df

        data[data_source['tag']] = Estimator.loaded_data[data_source['tag']]


      for tag, table in Estimator.database_tag_map.items():
        if not tag in Estimator.loaded_data:
          print("Loading " + tag)
          data[tag] = pd.read_sql_query("SELECT * FROM tabular." + table, Estimator.db_engine)
     

      for tag, table in data.items():
        if 'municipal' in table.columns:
          data[tag] = table[~table['municipal'].str.lower().isin(lowercase_blacklist)]


      return fn(data)

    return estimator
