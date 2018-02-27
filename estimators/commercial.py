"""
  Commercial Sector Estimator
"""

import pandas as pd
import numpy as np
from functools import reduce
from .estimator import Estimator


def commercial(data_sources):
  """
    @param List<Dict<String>> data_sources

    @return DataFrame
  """

  pba_naics_groups = {
    'education': [611],
    'food sales': [445],
    'food service': [722],
    'health care outpatient': [621],
    'lodging': [623, 721],
    'mercantile retail (other than mall)': [441, 442, 443, 444, 451, 452, 453, 532],
    'mercantile enclosed and strip malls': [446, 448],
    'office': [454, 486, 511, 516, 517, 518, 519, 521, 522, 523, 524, 525, 531, 533, 541, 551, 561, 624, 921, 923, 924, 925, 926, 928],
    'public assembly': [481, 482, 485, 487, 512, 515, 711, 712, 713],
    'religious worship': [813],
    'service': [447, 483, 484, 488, 491, 492, 811, 812],
    'warehouse and storage': [423, 424, 493],
  }
  
  fuel_types = ['elec', 'ng', 'foil']

  fuel_conversion = {
    'elec': 0.006707,
    'ng': 0.1,
    'foil': 0.139,
  }

  fuel_factor = {
    'elec': 1000,
    'ng': 9.64320154,
    'foil': 1,
  }

  co2_conversion_map = {
    'elec': 0.857,
    'ng': 11.71,
    'foil': 22.579,
  }

  col_order = [
    'muni_id',
    'municipal',
    'activity',
    'elec_con_per_b',
    'elec_exp_per_b',
    'elec_con_per_w',
    'elec_exp_per_w',
    'ng_con_per_b',
    'ng_exp_per_b',
    'ng_con_per_w',
    'ng_exp_per_w',
    'foil_con_per_b',
    'foil_exp_per_b',
    'foil_con_per_w',
    'foil_exp_per_w',
    'emps',
    'estabs',
    'elec_con_pu',
    'elec_exp_dollar',
    'elec_con_mmbtu',
    'elec_emissions_co2',
    'ng_con_pu',
    'ng_exp_dollar',
    'ng_con_mmbtu',
    'ng_emissions_co2',
    'foil_con_pu',
    'foil_exp_dollar',
    'foil_con_mmbtu',
    'foil_emissions_co2',
    'total_con_mmbtu'
  ]


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
    eowld_snapshot = eowld[['muni_id', 'municipal', 'naicscode', 'avgemp', 'estab']]

    results = pd.DataFrame()

    for municipality in eowld['municipal'].unique():
      eowld = pd.DataFrame(eowld_snapshot[eowld_snapshot['municipal'] == municipality])
      muni_id = eowld['muni_id'].unique()[0]

      pba_stats = {}
      for pba, naics_codes in pba_naics_groups.items():
        pba_stats[pba] = eowld[eowld['naicscode'].astype(int).isin(naics_codes)].sum()[['avgemp', 'estab']].fillna(0)


      """
        Step 2 in Methodology
      """

      # Pull in the CBECS datasets
      cbecs = {}
      for fuel in fuel_types:
        column_map = {
          'c_blg': fuel+'_con_per_b',
          'e_blg': fuel+'_exp_per_b',
          'c_perwrkr': fuel+'_con_per_w', # Physical Unit
          'e_kwh': fuel+'_exp_per_kwh', # Dollars
        }


        cbecs[fuel] = pd.DataFrame(datasets['cbecs_'+fuel][['activity'] + list(column_map)])
        cbecs[fuel]['activity'] = cbecs[fuel]['activity'].str.strip().str.lower()
        cbecs[fuel].rename(columns=column_map, inplace=True)
        cbecs[fuel][fuel+'_con_per_b'] = cbecs[fuel][fuel+'_con_per_b'].apply(pd.to_numeric)

        # Use Option 2 in methodology for replacing missing values in each column
        for column_name in column_map.values():
          column_avg = cbecs[fuel][column_name].mean()
          cbecs[fuel][column_name].fillna(column_avg, inplace=True)


      # Compose the datasets into a single DataFrame 
      current_result = None
      for i, fuel in enumerate(fuel_types):
        if i == 0:
          current_result = pd.DataFrame(cbecs[fuel][cbecs[fuel]['activity'].isin(pba_naics_groups.keys())])
        else:
          current_result = pd.merge(current_result, cbecs[fuel], on='activity')
        
      current_result['emps'] = current_result['activity'].apply(lambda x: pba_stats[x.strip()]['avgemp'])
      current_result['estabs'] = current_result['activity'].apply(lambda x: pba_stats[x.strip()]['estab'])

      for fuel in fuel_types:
        current_result[fuel+'_exp_per_w'] = (current_result[fuel+'_con_per_w'] * 1000) * current_result[fuel+'_exp_per_kwh']
        current_result[fuel+'_exp_per_w'].replace(np.inf, 0, inplace=True)


      # Find percentage of consumption for each fuel type.
      energy_sources_column_map = {
        'bld_indic': 'activity',
        'all_bldg': 'all'
      }

      source_column_map = {
        'nat_gas': 'ng',
        'fuel_oil': 'foil'
      }

      renamed_sources = {
        'outpatient': 'health care outpatient',
        'retail (other than mall)': 'mercantile retail (other than mall)',
        'enclosed and strip malls': 'mercantile enclosed and strip malls',
      }

      energy_sources_column_map.update(source_column_map)

      energy_sources = pd.DataFrame(datasets['cbecs_sources'][['years', 'bld_group', 'bld_indic', 'all_bldg', 'nat_gas', 'fuel_oil']])
      energy_sources = energy_sources[(energy_sources['bld_group'].str.lower() == 'principal building activity') & (energy_sources['years'].astype(int) == 2012)]
      energy_sources.rename(columns=energy_sources_column_map, inplace=True)
      energy_sources['activity'] = energy_sources['activity'].str.lower().str.strip()

      for original_source, renamed_source in renamed_sources.items():
        energy_sources['activity'].replace(original_source, renamed_source, inplace=True) 

      activities = current_result['activity'].tolist()
      energy_sources = energy_sources[energy_sources['activity'].isin(activities)].reset_index()
      energy_sources['foil'].fillna(0, inplace=True)

      for fuel in source_column_map.values():
        energy_sources[fuel] = energy_sources[fuel].astype(float) / energy_sources['all'].astype(float)

      energy_sources['elec'] = 1.0


      # Calculate avergage consumption and expenditure 

      result_sets = {
        'mercantile': pd.DataFrame(current_result[current_result['activity'] == 'Mercantile Enclosed and strip malls']),
        'non_mercantile': pd.DataFrame(current_result[current_result['activity'] != 'Mercantile Enclosed and strip malls'])
      }

      for fuel in fuel_types:

        for set_name, result_set in result_sets.items():

          if not result_set.empty:
            if set_name == 'mercantile':
              result_set[fuel+'_con_pu'] = result_set[fuel+'_con_per_b'] * result_set['estabs'] * energy_sources[fuel] * fuel_factor[fuel]
              result_set[fuel+'_exp_dollar'] = result_set[fuel+'_exp_per_b'] * result_set[fuel+'_con_pu']
            else:
              result_set[fuel+'_con_pu'] = result_set[fuel+'_con_per_w'] * result_set['emps'] * energy_sources[fuel] * fuel_factor[fuel]
              result_set[fuel+'_exp_dollar'] = result_set[fuel+'_exp_per_w'] * result_set[fuel+'_con_pu']

            result_set[fuel+'_con_mmbtu'] = result_set[fuel+'_con_pu'] * fuel_conversion[fuel]
            result_set[fuel+'_emissions_co2'] = result_set[fuel+'_con_pu'] * co2_conversion_map[fuel]


      current_result = pd.concat(result_sets).sort_values(['activity']).reset_index()
      del current_result['level_0']
      del current_result['level_1']

      current_result['total_con_mmbtu'] = current_result['elec_con_mmbtu'] + current_result['ng_con_mmbtu'] + current_result['foil_con_mmbtu']
      current_result['muni_id'] = muni_id
      current_result['municipal'] = municipality

      results = results.append(current_result, ignore_index=True)


    results = results[col_order]
    results['activity'] = results['activity'].str.title()

    # Rename certain municipal identifiers to conform to the the data
    # used in the other sectors.
    results.replace('MAPC Region', 'MAPC', inplace=True)


    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
