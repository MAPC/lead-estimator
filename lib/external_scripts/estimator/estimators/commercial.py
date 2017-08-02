"""
  Commercial Sector Estimator
"""

import pandas as pd
from functools import reduce
from .estimator import Estimator


def commercial(data_sources):
  """
    @param List<Dict<String>> data_sources

    @return DataFrame
  """

  pba_naics_groups = {
    'Education': [611],
    'Food sales': [445],
    'Food service': [722],
    'Health care Outpatient': [621],
    'Lodging': [623, 721],
    'Mercantile Retail (other than mall)': [441, 442, 443, 444, 451, 452, 453, 532],
    'Mercantile Enclosed and strip malls': [446, 448],
    'Office': [454, 486, 511, 516, 517, 518, 519, 521, 522, 523, 524, 525, 531, 533, 541, 551, 561, 624, 921, 923, 924, 925, 926, 928],
    'Public assembly': [481, 482, 485, 487, 512, 515, 711, 712, 713],
    'Religious worship': [813],
    'Service': [447, 483, 484, 488, 491, 492, 811, 812],
    'Warehouse and storage': [423, 424, 493],
  }
  
  fuel_types = ['elec', 'ng', 'foil']

  fuel_conversion = {
    'elec': 0.003412,
    'ng': 0.1,
    'foil': 0.139,
  }

  fuel_factor = {
    'elec': 1000,
    'ng': 1,
    'foil': 1,
  }

  co2_conversion_map = {
    'elec': 0.828,
    'ng': 11.71 * 10,
    'foil': 22.38,
  }



  def methodology(datasets):
    """
      @param Dict<DataFrame> datasets

      @return DataFrame 
    """

    """
      Step 1 in Methodology
    """
    eowld = datasets['eowld']
    eowld = eowld[(eowld['naicscode'].astype(int) >= 400) & (eowld['naicscode'].astype(int) <= 1000) & (eowld['cal_year'].astype(int) == 2015)]
    eowld_snapshot = eowld[['municipal', 'naicscode', 'avgemp', 'estab']]

    results = pd.DataFrame()

    for municipality in eowld['municipal'].unique():
      eowld = pd.DataFrame(eowld_snapshot[eowld_snapshot['municipal'] == municipality])

      pba_stats = {}
      for pba, naics_codes in pba_naics_groups.items():
        pba_stats[pba] = eowld[eowld['naicscode'].isin(naics_codes)].sum()[['avgemp', 'estab']]


      """
        Step 2 in Methodology
      """

      # Pull in the CBECS datasets
      cbecs = {}
      for fuel in fuel_types:
        column_map = {
          'cnsperworker': fuel+'_con_per', # Physical Unit
          'experworker': fuel+'_exp_per',  # Dollars
        }

        cbecs[fuel] = pd.DataFrame(datasets['cbecs_'+fuel][['activity', 'cnsperworker', 'experworker']])
        cbecs[fuel].rename(columns=column_map, inplace=True)

        # Use Option 2 in methodology for replacing missing values in each column
        for column_name in column_map.values():
          column_avg = cbecs[fuel][column_name].mean()
          cbecs[fuel][column_name].fillna(column_avg, inplace=True)


      # Compose the datasets into a single DataFrame 
      current_result = None
      for i, fuel in enumerate(fuel_types):
        if i == 0:
          current_result = pd.DataFrame(cbecs[fuel][cbecs[fuel]['activity'].str.strip().isin(pba_naics_groups.keys())])
        else:
          current_result = pd.merge(current_result, cbecs[fuel], on='activity')

      current_result['emps'] = current_result['activity'].apply(lambda x: pba_stats[x.strip()]['avgemp'])
      current_result['estabs'] = current_result['activity'].apply(lambda x: pba_stats[x.strip()]['estab'])


      # Find percentage of consumption for each fuel type.
      source_column_map = {
        'ng': 'ng',
        'fueloil': 'foil'
      }
      
      energy_sources = pd.DataFrame(datasets['cbecs_sources'][['activity', 'all', 'ng', 'fueloil']])
      energy_sources['fueloil'].fillna(0, inplace=True)
      energy_sources.rename(columns=source_column_map, inplace=True)

      for fuel in source_column_map.values():
        energy_sources[fuel] = energy_sources[fuel].astype(float) / energy_sources['all'].astype(float)

      energy_sources['elec'] = 1.0

      # Calculate avergage consumption and expenditure 
      for fuel in fuel_types:
        current_result[fuel+'_con_pu'] = current_result[fuel+'_con_per'] * current_result['emps'] * energy_sources[fuel] * fuel_factor[fuel]
        current_result[fuel+'_con_MMBTU'] = current_result[fuel+'_con_pu'] * fuel_conversion[fuel]
        current_result[fuel+'_exp_dol_pu'] = current_result[fuel+'_exp_per'] * current_result[fuel+'_con_pu']
        current_result[fuel+'_exp_dol_MMBTU'] = current_result[fuel+'_exp_dol_pu'] / fuel_conversion[fuel]
        current_result[fuel+'_emissions'] = current_result[fuel+'_con_pu'] * co2_conversion_map[fuel]

      current_result['total_cons_MMBTU'] = current_result['elec_con_MMBTU'] + current_result['ng_con_MMBTU'] + current_result['foil_con_MMBTU']
      current_result['municipal'] = municipality
      results = results.append(current_result, ignore_index=True)

    results = results[['municipal'] + results.columns.values.tolist()[:-1]]
    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
