import pandas as pd
from .estimator import Estimator
from pprint import pprint

def commercial(data_source):

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
  
  fuel_types = ['el', 'ng', 'fo']

  fuel_conversion = {
    'el': 0.003412,
    'ng': 0.1,
    'fo': 0.139,
  }

  fuel_factor = {
    'el': 1000,
    'ng': 1,
    'fo': 1,
  }


  def methodology(datasets):

    """
      Step 1 in Methodology
    """
    eowld = datasets['eowld']
    eowld = eowld[eowld['municipal'].str.lower() == 'gloucester']
    eowld = eowld[(eowld['naicscode'].astype(int) >= 400) & (eowld['naicscode'].astype(int) <= 1000) & (eowld['cal_year'].astype(int) == 2015)]
    eowld = eowld[['naicscode', 'avgemp', 'estab']]

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
        'cnsperworker': fuel+'_con_per',
        'experworker': fuel+'_exp_per',
      }

      cbecs[fuel] = pd.DataFrame(datasets['cbecs_'+fuel][['activity', 'cnsperworker', 'experworker']])
      cbecs[fuel].rename(columns=column_map, inplace=True)

      # Use Option 2 in methodology for replacing missing values in each column
      for column_name in column_map.values():
        column_avg = cbecs[fuel][column_name].mean()
        cbecs[fuel][column_name].fillna(column_avg, inplace=True)


    # Compose the datasets into a single DataFrame 
    results = None
    for i, fuel in enumerate(fuel_types):
      if i == 0:
        results = pd.DataFrame(cbecs[fuel][cbecs[fuel]['activity'].str.strip().isin(pba_naics_groups.keys())])
      else:
        results = pd.merge(results, cbecs[fuel], on='activity')

    results['emps'] = results['activity'].apply(lambda x: pba_stats[x.strip()]['avgemp'])
    results['estabs'] = results['activity'].apply(lambda x: pba_stats[x.strip()]['estab'])


    # Find percentage of consumption for each fuel type.
    # Methodology says to keep the electrical consumption at 100%.
    fuel_con_percentage = {'el': 1.0}

    fuel_con_filter = []
    for fuel in fuel_types:
      fuel_con_filter.append(fuel+'_con_per')

    fuel_con_totals = results[fuel_con_filter].sum()
    fuel_con_matrix_total = fuel_con_totals.sum()

    for fuel in [x for x in fuel_types if x != 'el']:
      fuel_con_percentage[fuel] = fuel_con_totals[fuel+'_con_per'] / fuel_con_matrix_total

    
    # Calculate avergage consumption and expenditure 
    for fuel in fuel_types:
      results[fuel+'_con'] = results[fuel+'_con_per'] * results['emps'] * fuel_con_percentage[fuel] * fuel_factor[fuel]
      results[fuel+'_exp'] = results[fuel+'_exp_per'] * results['emps'] * fuel_factor[fuel] 


    pprint(results)
    return results

  return Estimator(methodology)(data_source)
