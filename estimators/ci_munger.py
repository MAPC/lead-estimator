"""
  Commercial & Industrial Sector Munger

  This Estimator will take the estimates from Commercial & Industrial sectors
  and will (1) scale them to the data collected by MassSave; (2) will calculate
  these scales for each year provided by MassSave.
"""

import pandas as pd
import numpy as np
from functools import reduce
from .estimator import Estimator


def ci_munger(data_sources, sector_data):
  """
    @param List<Dict<String>> data_sources
    @param <Dict<String>> sector_data

    @return <Dict<String>>
  """
  
  fuel_types = ['elec', 'ng']

  com_col_order = [
    'muni_id',
    'municipal',
    'year',
    'calibrated',
    'activity',
    'elec_con_pu',
    'elec_con_mmbtu',
    'elec_exp_dollar',
    'elec_emissions_co2',
    'ng_con_pu',
    'ng_con_mmbtu',
    'ng_exp_dollar',
    'ng_emissions_co2',
    'foil_con_pu',
    'foil_con_mmbtu',
    'foil_exp_dollar',
    'foil_emissions_co2',
    'total_con_mmbtu'
  ]

  ind_col_order = [
    'muni_id',
    'municipal',
    'year',
    'calibrated',
    'naicstitle',
    'elec_con_pu',
    'elec_con_mmbtu',
    'elec_exp_dollar',
    'elec_emissions_co2',
    'ng_con_pu',
    'ng_con_mmbtu',
    'ng_exp_dollar',
    'ng_emissions_co2',
    'foil_con_pu',
    'foil_con_mmbtu',
    'foil_exp_dollar',
    'foil_emissions_co2',
    'elec_con_perc',
    'ng_con_perc',
    'foil_con_perc',
    'total_con_mmbtu'
  ]

  fuel_conversion = {
    'elec': {
      2013: 0.006841,
      2014: 0.007692, 
      2015: 0.006707,
    },
    'ng': 0.1,
  }

  emissions_factors = {
    'elec': {
      2013: .93,     
      2014: .941,
      2015: .857,
    },
    'ng': 11.710
  }


  def methodology(datasets):
    """
      @param Dict<DataFrame> datasets

      @return DataFrame 
    """

    masssave_ci_all = pd.DataFrame(datasets['masssave_ci'])
    masssave_ci_all = masssave_ci_all[['municipal', 'cal_year', 'mwh_use', 'therm_use']].rename(columns={'mwh_use': 'elec', 'therm_use': 'ng'})
    masssave_ci_all['elec'] *= 1000

    years = masssave_ci_all['cal_year'].unique()
    latest_year = years[-1]

    sectors = {
      'commercial': pd.DataFrame(),
      'industrial': pd.DataFrame(),
    }

    for municipality in datasets['eowld']['municipal'].unique():
      masssave_ci = masssave_ci_all[masssave_ci_all['municipal'] == municipality]
      muni_data = {
        'commercial': sector_data['commercial'][sector_data['commercial']['municipal'] == municipality],
        'industrial': sector_data['industrial'][sector_data['industrial']['municipal'] == municipality],
      }

      pu_totals = {}
      for fuel in fuel_types:
        pu_totals[fuel] = muni_data['commercial'][fuel+'_con_pu'].sum() + muni_data['industrial'][fuel+'_con_pu'].sum()

      has_uncalibrated = {}
      for sector in sectors.keys():
        has_uncalibrated[sector] = False

      for year in years:
        masssave = masssave_ci[masssave_ci['cal_year'] == year]

        for sector in sectors.keys():
          muni_sector_data = muni_data[sector].copy()
          muni_sector_data['year'] = year

          if masssave.isnull().sum().sum() > 0:
            if has_uncalibrated[sector]:
              continue

            muni_sector_data['calibrated'] = False
            has_uncalibrated[sector] = True

          else:
            muni_sector_data['calibrated'] = True
            for fuel in fuel_types:
              ratio = (masssave[fuel] / pu_totals[fuel]).values
              calibrator = ratio[0] if len(ratio) > 0 and not np.isnan(ratio[0]) else 1

              muni_sector_data[fuel+'_con_pu'] = muni_sector_data[fuel+'_con_pu'].apply(lambda x: x * calibrator)
              muni_sector_data[fuel+'_exp_dollar'] = muni_sector_data[fuel+'_exp_dollar'].apply(lambda x: x * calibrator)
              muni_sector_data[fuel+'_con_mmbtu'] = muni_sector_data[fuel+'_con_pu'] * (fuel_conversion['elec'][year] or fuel_conversion['elec'][latest_year]) if fuel == 'elec' else muni_sector_data[fuel+'_con_pu'] * fuel_conversion[fuel]
              muni_sector_data[fuel+'_emissions_co2'] = muni_sector_data[fuel+'_con_pu'] * (emissions_factors['elec'][year] or emissions_factors['elec'][latest_year]) if fuel == 'elec' else muni_sector_data[fuel+'_con_pu'] * emissions_factors[fuel]

          sectors[sector] = sectors[sector].append(muni_sector_data, ignore_index=True)

    sectors['commercial'].sort_values(['municipal', 'year', 'activity'], inplace=True)
    sectors['industrial'].sort_values(['municipal', 'year', 'naics_code'], inplace=True)

    results = {
      'commercial': sectors['commercial'][com_col_order],
      'industrial': sectors['industrial'][ind_col_order],
      'residential': sector_data['residential']
    }

    return results


  # Construct the Estimator from the methodology and then process the data sources
  return Estimator(methodology)(data_sources)
