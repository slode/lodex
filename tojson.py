import sys
from liblodex import Lodex

db = Lodex("animals.ldx")
with open(sys.argv[1], 'r') as f:
    headers = f.readline().split("\t")
    db.add_index('MSW93_Order')
    db.add_index('MSW93_Family')
    db.add_index('MSW93_Genus')
    db.add_index('MSW93_Species')
    db.add_index('MSW93_Binomial')

    for line in f:
        db.put(dict([(k.rstrip(),v.rstrip()) for k,v in zip(headers, line.split("\t")) if not v.startswith("-999")]))
    db.commit()

