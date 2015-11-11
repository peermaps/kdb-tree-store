# page

SIZE bytes:

[TYPE][DATA...]

# region page (TYPE=0)

n regions (uint16)
each region: [ min (float32), max (float32), page id (uint32???) ]

# point page (TYPE=1)

n points (uint16)

[ coord0, coord1, ...coordN, LOCATION (uint64) ]

