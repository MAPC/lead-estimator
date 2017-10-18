"""
    This file contains municipalities that are in the dataset that
    we do not want to appear in the application. We remove these 
    municipalities at the munging level since we want the dataset 
    to fully represent what the app has to offer and nothing more.
"""

blacklist = [
    'Barnstable County',
    'Berkshire County',
    'Bristol County',
    'Dukes County',
    'Essex County',
    'Established Suburb/Cape Cod Town',
    'Franklin County',
    'Hampden County',
    'Hampshire County',
    'Middlesex County',
    'Nantucket County',
    'Norfolk County',
    'Plymouth County',
    'Suffolk County',
    'Worcester County',
    'Franklin Regional Council Of Governments',
    'Inner Core (ICC) Subregion',
    'MAGIC Subregion',
    'MetroFuture',
    'MetroFuture Region',
    'Northern Middlesex Council of Government',
    'North Shore (NSTF) Subregion',
    'North Suburban (NSPC) Subregion',
    'South Shore (SSC) Subregion',
    'South West (SWAP) Subregion',
    'Sub-Regional Urban Center',
    'Three Rivers (TRIC) Subregion',
]
