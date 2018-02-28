"""
  Industrial Sector Estimator
"""

import pandas as pd
import numpy as np
from .estimator import Estimator
from pprint import pprint


def industrial(data_sources):
  """
    @param List<Dict<String>> data_sources

    @return DataFrame
  """

  fuel_types = ['elec', 'foil', 'ng', 'other']

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
    mecs_fce.rename(columns={'naicscode': 'naics_code'}, inplace=True)
    mecs_fce['naics_code'] = mecs_fce['naics_code'].astype(int)

    mecs_fce = mecs_fce[(mecs_fce['naics_code'].isin(naics_codes)) & (mecs_fce['years'] == 2010) & (mecs_fce['geography'].str.lower() == 'northeast region')]
    mecs_fce = mecs_fce[['naics_code', 'c_employee']]

    results = pd.merge(results, mecs_fce, on='naics_code')
    replace_invalid_values(results)

    results['total_con_mmbtu'] = results['c_employee'].astype(float) * results['avgemp'].astype(float)


    """
      Step 3 in Methodology
    """
    mecs_ami = pd.DataFrame(datasets['mecs_ami'])
    mecs_ami.rename(columns={'naics_3d': 'naics_code'}, inplace=True)

    mecs_ami['naics_code'] = mecs_ami['naics_code'].apply(pd.to_numeric, errors='coerce')
    mecs_ami = mecs_ami[(mecs_ami['naics_code'].isin(naics_codes)) & (mecs_ami['geography'].str.lower() == 'northeast') & (mecs_ami['years'] == 2010)]
    replace_invalid_values(mecs_ami)

    mecs_ami['other'] = mecs_ami[['lpg_ngl', 'coal', 'coke_brz', 'other']].apply(pd.to_numeric).sum(axis=1, skipna=True)
    mecs_ami['foil'] = mecs_ami[['d_fueloil', 'r_fueloil']].apply(pd.to_numeric).sum(axis=1, skipna=True)
    mecs_ami = mecs_ami[['net_elec', 'natgas', 'foil', 'other', 'tot_consum', 'naics_code']].rename(columns={'net_elec': 'elec', 'natgas': 'ng', 'tot_consum': 'tot'})

    for fuel in fuel_types:
      mecs_ami[fuel+'_con_perc'] = mecs_ami[fuel].astype(float) / mecs_ami['tot'].astype(float)

    mecs_ami['naics_code'] = mecs_ami['naics_code'].astype(int)
    mecs_ami.drop(fuel_types + ['tot'], axis=1, inplace=True)

    results = pd.merge(results, mecs_ami, on='naics_code')

    for fuel in fuel_types:
      results[fuel+'_con_mmbtu'] = results['total_con_mmbtu'] * results[fuel+'_con_perc']

      if not fuel == 'other':
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
