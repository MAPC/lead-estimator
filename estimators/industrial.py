"""
  Industrial Sector Estimator
"""

import pandas as pd
import numpy as np
from .estimator import Estimator


def industrial(data_sources):
  """
    @param List<Dict<String>> data_sources

    @return DataFrame
  """

  fuel_types = ['elec', 'foil', 'ng']

  fuel_conversion = {
    'elec': 0.006707,
    'ng': 0.1,
    'foil': 0.139,
  }

  exp_per_fuel_pu = {
    'elec': 0.078,
    'ng': 7.77,
    'foil': 1.11,
  }

  co2_conversion_map = {
    'elec': 0.857,
    'ng': 11.71,
    'foil': 22.579,
  }


  def replace_invalid_values(df):
    """
      @param DataFrame df
    """

    df.replace('*', np.nan, inplace=True)
    df.replace('Q', np.nan, inplace=True)


  def methodology(datasets):
    """
      @param Dict<DataFrame> datasets

      @return DataFrame
    """

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 800)

    """
      Step 1 in Methodology
    """
    eowld = pd.DataFrame(datasets['eowld'])
    eowld['naicscode'] = eowld['naicscode'].astype(int)
    eowld = eowld[(eowld['naicscode'] >= 311) & (eowld['naicscode'] <= 339) & (eowld['cal_year'].astype(int) == 2015)]
    eowld = eowld[['muni_id', 'municipal', 'naicscode', 'naicstitle', 'avgemp', 'estab']]
    eowld = eowld.sort_values(['naicscode']) 
    eowld.rename(columns={'naicscode': 'naics_code'}, inplace=True)

    # We need the NAICS codes to filter the remaining datasets
    naics_codes = list(eowld[['naics_code']].values.T.flatten())

    results = eowld


    """
      Step 2 in Methodology
    """

    mecs_fce = pd.DataFrame(datasets['mecs_fce'])
    mecs_fce.rename(columns={'naicscode': 'naics_code', 'c_employee': 'con_per_w'}, inplace=True)
    mecs_fce['naics_code'] = mecs_fce['naics_code'].astype(int)

    mecs_fce = mecs_fce[(mecs_fce['naics_code'].isin(naics_codes)) & (mecs_fce['years'] == 2010) & (mecs_fce['geography'].str.lower() == 'northeast region')]
    mecs_fce = mecs_fce[['naics_code', 'con_per_w']]

    results = pd.merge(results, mecs_fce, on='naics_code')
    replace_invalid_values(results)

    results['total_con_mmbtu'] = results['con_per_w'].astype(float) * results['avgemp'].astype(float)


    """
      Step 3 in Methodology
    """

    mecs_data = {
      'euc': pd.DataFrame(datasets['mecs_euc']),
      'fuc': pd.DataFrame(datasets['mecs_fuc']),
    }

    for dataset in mecs_data.keys():
      mecs_data[dataset].rename(columns={'naics_3d': 'naics_code'}, inplace=True)
      mecs_data[dataset]['naics_code'] = mecs_data[dataset]['naics_code'].apply(pd.to_numeric, errors='coerce')
      mecs_data[dataset] = mecs_data[dataset][(mecs_data[dataset]['naics_code'].isin(naics_codes)) & (mecs_data[dataset]['geography'].str.lower() == 'united states') & (mecs_data[dataset]['years'] == 2010)]
      replace_invalid_values(mecs_data[dataset])

    mecs_data['euc']['foil'] = mecs_data[dataset][['d_fueloil', 'r_fueloil']].apply(pd.to_numeric).sum(axis=1, skipna=True)
    mecs_data['euc'] = mecs_data['euc'][['net_elec', 'natgas', 'foil', 'naics_code']].rename(columns={'net_elec': 'elec', 'natgas': 'ng'})
    mecs_data['fuc'] = mecs_data['fuc'][['naics_code', 'tot_consum']].rename(columns={'tot_consum': 'tot'})

    mecs = pd.merge(mecs_data['euc'], mecs_data['fuc'], on="naics_code")

    for fuel in fuel_types:
      mecs[fuel+'_con_perc'] = mecs[fuel].astype(float) / mecs['tot'].astype(float)

    mecs['naics_code'] = mecs['naics_code'].astype(int)
    mecs.drop(fuel_types + ['tot'], axis=1, inplace=True)
    replace_invalid_values(mecs)

    results = pd.merge(results, mecs, on='naics_code')

    for fuel in fuel_types:
      results[fuel+'_con_mmbtu'] = results['total_con_mmbtu'] * results[fuel+'_con_perc']
      results[fuel+'_con_pu'] = results[fuel+'_con_mmbtu'] / fuel_conversion[fuel]
      results[fuel+'_exp_dollar'] = results[fuel+'_con_pu'] * exp_per_fuel_pu[fuel]
      results[fuel+'_emissions_co2'] = results[fuel+'_con_pu'] * co2_conversion_map[fuel]

    results.sort_values('municipal', inplace=True)

    # Rename certain municipal identifiers to conform to the the data
    # used in the other sectors.
    results.replace('MAPC Region', 'MAPC', inplace=True)

    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
