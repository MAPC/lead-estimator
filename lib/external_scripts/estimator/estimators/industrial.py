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

  fuel_types = ['elec', 'foil', 'ng', 'other']


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
    eowld = datasets['eowld']
    eowld = eowld[(eowld['naicscode'].astype(int) >= 311) & (eowld['naicscode'].astype(int) <= 339) & (eowld['cal_year'].astype(int) == 2015)]
    eowld = eowld[['municipal', 'naicscode', 'naicstitle', 'avgemp', 'estab']]
    eowld = eowld.sort_values(['naicscode']) 
    eowld.rename(columns={'naicscode': 'naics_code'}, inplace=True)

    # We need the NAICS codes to filter the remaining datasets
    naics_codes = list(eowld[['naics_code']].values.T.flatten())

    results = eowld


    """
      Step 2 in Methodology
    """
    mecs_fce = datasets['mecs_fce']
    mecs_fce = mecs_fce[(mecs_fce['naics_code'].isin(naics_codes)) & (mecs_fce['region'].str.lower() == 'northeast')]

    mecs_fce = mecs_fce[['naics_code', 'cons_emp']]

    results = pd.merge(results, mecs_fce, on='naics_code')
    replace_invalid_values(results)
    results['total_con_mmbtu'] = results['cons_emp'].str.replace(',','').astype(float) * results['avgemp'].astype(float)


    """
      Step 3 in Methodology
    """
    mecs_ami = pd.DataFrame(datasets['mecs_ami'])
    mecs_ami['naics_code'] = mecs_ami['naics_code'].apply(pd.to_numeric, errors='coerce')
    mecs_ami = mecs_ami[(mecs_ami['naics_code'].isin(naics_codes)) & (mecs_ami['geography'].str.lower() == 'northeast')]
    replace_invalid_values(mecs_ami)

    mecs_ami['other'] = mecs_ami[['lpgngl', 'coal', 'coke', 'other']].apply(pd.to_numeric).sum(axis=1, skipna=True)
    mecs_ami['foil'] = mecs_ami[['dist_foil', 'res_foil']].apply(pd.to_numeric).sum(axis=1, skipna=True)
    mecs_ami = mecs_ami[fuel_types + ['tot', 'naics_code']]

    for fuel in fuel_types:
      mecs_ami[fuel+'_con_%'] = mecs_ami[fuel].astype(float) / mecs_ami['tot'].astype(float)

    mecs_ami.drop(fuel_types + ['tot'], axis=1, inplace=True)
    results = pd.merge(results, mecs_ami, on='naics_code')

    for fuel in fuel_types:
      results[fuel+'_con_mmbtu'] = results['total_con_mmbtu'] * results[fuel+'_con_%']

    results.sort_values('municipal', inplace=True)


    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
