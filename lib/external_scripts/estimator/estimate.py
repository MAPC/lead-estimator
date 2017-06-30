#!/usr/bin/env python3

from getopt import getopt
import sys
from pprint import pprint
from functools import reduce
import estimators

# Get command line arguments
short_options = 's:f:t:'
long_options  = ['sector=', 'file=', 'tag=']

options = getopt(sys.argv[1:], short_options, long_options)[0]


data_processors = {
  'commercial': estimators.commercial,
  'residential': estimators.residential,
  'industrial': estimators.industrial
}


data_files = []
sector = None


# Grab the argument values from our options list
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


if not sector or sector == 'all':
  # if no sector is specified, then we proceed to process
  # the data for all sectors
  sector_data = {}

  for sec, processor in data_processors.items():
    sector_data[sec] = processor(data_files)

else:

  if sector in data_processors:
    sector_data = {str(sector): data_processors[sector](data_files)}

  else:
    valid_sectors = reduce(lambda x,y: x+', '+y, data_processors.keys())
    print("{0} is not a valid sector! Valid sector arguments are {1}.".format(sector, valid_sectors))
