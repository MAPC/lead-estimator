"""
  Residential Sector Estimator
"""

import pandas as pd
import numpy as np
from .estimator import Estimator
 

def residential(data_sources):
  """
    @param List<Dict<String>> data_sources

    @return DataFrame
  """

  fuel_type_map = {
    'gas': 'ng',
    'oil': 'foil',
    'elec': 'elec',
  }

  fuel_cons_columns = [x+'_con_mmbtu' for x in fuel_type_map.values()]
  fuel_cons_pu_columns = [x+'_con_pu' for x in fuel_type_map.values()]
  fuel_exp_columns = [x+'_exp_dollar' for x in fuel_type_map.values()]

  # Build two additional maps from our fuel types
  fuel_avg_map = {}
  hfc_fuel_map = {}
  hfe_fuel_map = {}
  for fuel in fuel_type_map.values():
    fuel_avg_map['avg_'+fuel] = fuel
    hfc_fuel_map[fuel] = fuel+'_hfc'
    hfe_fuel_map[fuel] = fuel+'_hfe'


  hu_type_map = {
      'Single_family_attached': 'u1a',
      'Single_family_detached': 'u1d',
      'two_four_units': 'u2_4',
      'five_plus_units': 'u5ov',
      'Mobile_home': 'u_oth',
  }

  hfc_hu_map = {
    'Total Households': 'total',
    'Single-Family Attached': 'u1a',
    'Single-Family Detached': 'u1d',
    'Apartments in 2-4 Unit Buildings': 'u2_4',
    'Apartments in 5 or More Unit Buildings': 'u5ov',
    'Mobile Homes': 'u_oth',
  }

  hfe_hu_map = {
    'total households': 'total',
    'Single family attached': 'u1a',
    'Single family detached': 'u1d',
    'Apartments in 2-4 Unit Buildings': 'u2_4',
    'Apartments in 5 or More Unit Buildings': 'u5ov',
    'mobile homes': 'u_oth',
  }

  fuel_conversion_map = {
    'elec': 0.003412,
    'ng': 0.1 * 10,
    'foil': 0.139,
  }

  co2_conversion_map = {
    'elec': 0.828,
    'ng': 11.71,
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
    acs_uis = datasets['acs_uis']
    acs_uis = acs_uis[(acs_uis['acs_year'] == '2011-15')]
    acs_uis = acs_uis[['muni_id', 'municipal', 'hu', 'u1a', 'u1d', 'u2_4', 'u5_9', 'u10_19', 'u20ov', 'u_oth']]
    acs_uis.rename(columns={'hu': 'total'}, inplace=True)

    # Add 5 and over columns together
    u5ov = ['u5_9', 'u10_19', 'u20ov']
    acs_uis['u5ov'] = acs_uis[u5ov].sum(axis=1, skipna=True)
    acs_uis.drop(u5ov, axis=1, inplace=True)



    """
      Step 2 in Methodology
    """
    acs_hf = datasets['acs_hf']
    acs_hf = acs_hf[(acs_hf['acs_year'] == '2011-15')]
    acs_hf = acs_hf[['muni_id', 'gas', 'elec', 'oil']]
    
    results = pd.merge(acs_uis, acs_hf, on='muni_id')
    
    for fuel_og, fuel in fuel_type_map.items():
      results[fuel+'_%'] = results[fuel_og] / results['total']


    """
      Step 3 in Methodology
    """
    # Prepare percentages to scale the energy consumption for MA 
    # based on the national 
    recs_sc = pd.DataFrame(datasets['recs_sc'][['hu_type', 'ma']])
    #recs_sc.replace('Single_family.*', 'Single_family', regex=True, inplace=True)
    recs_sc.replace('Q', np.nan, inplace=True)
    recs_sc['hu_type'] = recs_sc['hu_type'].map(hu_type_map)
    recs_sc['ma'] = recs_sc['ma'].astype(float)
    ma_sum = recs_sc[['ma']].sum()
    recs_sc = recs_sc.append(pd.DataFrame({'hu_type': 'total', 'ma': ma_sum}))
    recs_sc = recs_sc.groupby('hu_type').sum()
    recs_sc = recs_sc.reset_index()

    recs_sc['ma'] = recs_sc['ma'] / float(ma_sum)

    # Apply MA percentages to national energy consumption
    recs_hfc = pd.DataFrame(datasets['recs_hfc'])
    recs_hfe = pd.DataFrame(datasets['recs_hfe'])

    recs_hfc_ma = recs_hfc[recs_hfc['geography'].str.lower() == 'massachusetts']
    recs_hfc = recs_hfc[(recs_hfc['geography'].str.lower() == 'united states')]
    recs_hfe = recs_hfe[recs_hfe['geography'].str.lower() == 'united states']

    recs_hfc = recs_hfc[['hu_type', 'avg_elec', 'avg_ng', 'avg_foil']]
    recs_hfe = recs_hfe[['hu_type', 'avg_elec', 'avg_ng', 'avg_foil']]

    recs_hfc.rename(columns=fuel_avg_map, inplace=True)
    recs_hfe.rename(columns=fuel_avg_map, inplace=True)

    recs_hfc['hu_type'] = recs_hfc['hu_type'].map(hfc_hu_map)
    recs_hfe['hu_type'] = recs_hfe['hu_type'].map(hfe_hu_map)

    recs_hfc = recs_hfc.groupby('hu_type').sum()
    recs_hfc = recs_hfc.reset_index()
    recs_hfc = pd.merge(recs_hfc, recs_sc, on='hu_type')

    for fuel in fuel_type_map.values():
      recs_hfe[fuel] = recs_hfe[fuel].str.replace(',', '').apply(pd.to_numeric)

    recs_hfe = recs_hfe.groupby('hu_type').sum()
    recs_hfe = recs_hfe.reset_index()
    recs_hfe = pd.merge(recs_hfe, recs_sc, on='hu_type')

    ma_consumptions = {
      'elec': recs_hfc_ma['avg_elec'],
      'ng': recs_hfc_ma['avg_ng'],
      'foil': recs_hfc_ma['avg_foil'],
    }

    for fuel in fuel_type_map.values():
      recs_hfc['adj'] = recs_hfc[fuel] * recs_hfc['ma']
      adjustment_ratio = (ma_consumptions[fuel] / recs_hfc[recs_hfc['hu_type'] != 'total']['adj'].sum()).values[0]
      recs_hfc[fuel] = recs_hfc[fuel].astype(float) * adjustment_ratio
      recs_hfe[fuel] = recs_hfe[fuel].astype(float) * adjustment_ratio

    recs_hfc.drop(['adj', 'ma'], axis=1, inplace=True)
    recs_hfc.rename(columns=hfc_fuel_map, inplace=True)
    recs_hfe.rename(columns=hfe_fuel_map, inplace=True)

    results = pd.melt(results, id_vars=['muni_id', 'municipal', 'gas', 'elec', 'oil', 'ng_%', 'foil_%', 'elec_%'], var_name='hu_type', value_name='hu')
    results.rename(columns=fuel_type_map, inplace=True)
    results = pd.merge(results, recs_hfc, on='hu_type')
    results = pd.merge(results, recs_hfe, on='hu_type')


    """
      Step 4 in Methodology
    """
    for fuel in fuel_type_map.values():
      results[fuel+'_con_mmbtu'] = results[fuel] * results[fuel+'_hfc']
      results[fuel+'_con_pu'] = results[fuel+'_con_mmbtu'] / fuel_conversion_map[fuel]

      results[fuel+'_exp_dollar'] = results[fuel] * results[fuel+'_hfe']


    results = results[['muni_id', 'municipal', 'hu_type', 'hu'] + fuel_cons_columns + fuel_cons_pu_columns + fuel_exp_columns]
    results['con_by_structure_mmbtu'] = results[fuel_cons_columns].sum(axis=1)
    results['exp_by_structure_dollar'] = results[fuel_exp_columns].sum(axis=1)

    emissions = pd.DataFrame()
    for fuel, conversion_ratio in co2_conversion_map.items():
      results[fuel+'_emissions_co2'] = results[fuel+'_con_pu'] * conversion_ratio

    results.sort_values(['municipal', 'hu_type'], inplace=True)

    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
