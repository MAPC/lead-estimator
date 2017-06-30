import pandas as pd
from pprint import pprint
from .estimator import Estimator

def industrial(data_sources):

  def methodology(datasets):

    """
      Step 1 in Methodology
    """
    eowld = datasets['eowld']
    eowld = eowld[eowld['municipal'].str.lower() == 'gloucester']
    eowld = eowld[(eowld['naicscode'].astype(int) >= 311) & (eowld['naicscode'].astype(int) <= 339) & (eowld['cal_year'].astype(int) == 2015)]
    eowld = eowld[['naicscode', 'naicstitle', 'avgemp', 'estab']]
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
    results['cons_total'] = results['cons_emp'].str.replace(',','').astype(float) * results['avgemp'].astype(float)


    """
      Step 3 in Methodology
    """
    mecs_ami = datasets['mecs_ami']
    mecs_ami['naics_code'] = mecs_ami['naics_code'].apply(pd.to_numeric, errors='coerce')
    mecs_ami = mecs_ami[mecs_ami['naics_code'].isin(naics_codes)]

    #mecs_ami['cons_total'] = mecs_ami[['elec', 'res_foil', '']]

    pprint(mecs_ami)

    #mecs_fci = mecs_fci[(mecs_fci['naics_code'].isin(naics_codes)) & (mecs_fci['region'].str.lower() == 'northeast')]


    return results

  return Estimator(methodology)(data_sources)
