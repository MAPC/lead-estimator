#!/usr/bin/env python3

"""
  estimate.py

  Implements the methodologies for each sector as outlined in the 
  'Estimate Local Energy Use Baseline' document.

  Arguments:

    --file, -f:     The absolute path to a data file that is in either .csv, .xls, or .xlsx format.
                    These optional files are used in place of tables in the database when properly 
                    tagged. See --tag argument.

    --sector, -s:   The name of the sector that will be processed. If this argument is not included 
                    in the command, then the program will process all sectors.

    --tag, -t:      This argument must follow a --file argument. If this argument does not follow a 
                    --file argument, the file will not be used. Possible file tags are eowld, 
                    cbecs_el, cbecs_fo, cbecs_ng, cbecs_sources, mecs_ami, mecs_fce, mecs_fci,
                    recs_hfc, recs_hfe, recs_sc, acs_uis, acs_hf. See --file argument.
"""

import sys
import estimators
from getopt import getopt
from pprint import pprint
from functools import reduce


# Get command line arguments
short_options = 's:f:t:'
long_options  = ['sector=', 'file=', 'tag=']

options = getopt(sys.argv[1:], short_options, long_options)[0]


data_processors = {
  'commercial': estimators.commercial,
  'residential': estimators.residential,
  'industrial': estimators.industrial
}


# Grab the argument values from our options list
data_files = []
sector = None

check_for_tag = False
for opt, arg in options:

  if check_for_tag:
    check_for_tag = False
    if opt in ['-t', '--tag']:
      data_files[-1]['tag'] = arg.strip()
      continue

  if opt in ['-s', '--sector']:
    sector = arg.strip()
  elif opt in ['-f', '--file']:
    data_files.append({'file_path': arg.strip(), 'tag': ''})

    # Whenever a --file argument is passed, a tag for that file should follow
    check_for_tag = True 


if not len(data_files) > 0 :
  print('You must pass the data file being used for input! e.g. estimate.py --file=data.xls')
  exit()


# Start processing the sector(s)
if not sector or sector == 'all':

  # if no sector is specified, then we proceed to process
  # the data for all sectors
  sector_data = {}

  for sec, processor in data_processors.items():
    sector_data[sec] = processor(data_files).to_json()

else:

  if sector in data_processors:
    sector_data = {str(sector): data_processors[sector](data_files)}

  else:
    valid_sectors = reduce(lambda x,y: x+', '+y, data_processors.keys())
    print("{0} is not a valid sector! Valid sector arguments are {1}.".format(sector, valid_sectors))


# Print data to be piped into a file or stdin
print(sector_data)
