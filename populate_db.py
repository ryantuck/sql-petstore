from db import *
import json
import os
import random
import arrow

from sqlalchemy.sql import text

# setup
dir_path = os.path.dirname(os.path.abspath(__file__))

with open(dir_path + '/db_config.json') as f:
    db_config = json.load(f)

db = DsDb(db_config['petstore'])


tables = [
        'customers',
        'pets',
        'items',
        'manufacturers',
        'transactions',
        'transaction_items'
        ]

presets = [
        'customers',
        'items',
        'manufacturers'
        ]

generated = {
        'pets': {
            'cols': [
                'id',
                'name',
                'species',
                'weight',
                'age',
                'owner_id'
                ],
            'num_records': 80,
            'start_id': 100
            },
        'transactions': {
            'cols': [
                'id',
                'customer_id',
                'date'
                ],
            'num_records': 200,
            'start_id': 1000
            },
        'transaction_items': {
            'cols': [
                'item_id',
                'quantity',
                'transaction_id'
                ],
            'num_records': 500,
            'start_id': 10000
            }
        }

pet_species = [
        'cat',
        'dog',
        'fish',
        'gerbil',
        'mouse',
        'tyrannosaurus rex'
        ]

with open(dir_path + '/pet_names.txt') as f:
    pet_names = f.read().splitlines()

dfs = {}

for t in presets:
    csv_file = ''.join(('csv/',t,'.csv'))
    df = pd.read_csv(dir_path + '/' + csv_file)
    dfs[t] = df

for k,v in generated.iteritems():
    dfs[k] = pd.DataFrame(None, columns=v['cols'])

# create pets dataframe
start = generated['pets']['start_id']
end = start + generated['pets']['num_records']
pet_ids = range(start, end)
dfs['pets']['id'] = pd.Series(pet_ids)

dfs['pets']['name'] = dfs['pets']['name'].apply(lambda x: random.choice(pet_names))
dfs['pets']['species'] = dfs['pets']['species'].apply(lambda x: random.choice(pet_species))
dfs['pets']['weight'] = dfs['pets']['weight'].apply(lambda x: random.randrange(1,2000))
dfs['pets']['age'] = dfs['pets']['age'].apply(lambda x: random.randrange(1,100))

customer_ids = list(dfs['customers']['id'])
dfs['pets']['owner_id'] = dfs['pets']['owner_id'].apply(lambda x: random.choice(customer_ids))

# create transactions dataframe
start = generated['transactions']['start_id']
end = start + generated['transactions']['num_records']
transaction_ids = range(start, end)
dfs['transactions']['id'] = pd.Series(transaction_ids)

dfs['transactions']['customer_id'] = dfs['transactions']['customer_id'].apply(
        lambda x: random.choice(customer_ids)
        )
dfs['transactions']['date'] = dfs['transactions']['date'].apply(
        lambda x:
            arrow.get(
                random.randrange(2010,2015),
                random.randrange(1,12),
                random.randrange(1,28)
                ).format('YYYY-MM-DD')
        )

# create transaction_items dataframe
start = generated['transaction_items']['start_id']
end = start + generated['transaction_items']['num_records']
ids = range(start, end)

item_ids = list(dfs['items']['id'])
transaction_ids = list(dfs['transactions']['id'])
random_item_ids = [random.choice(item_ids) for x in ids]
dfs['transaction_items']['item_id'] = pd.Series(random_item_ids)
t_ids = transaction_ids + [random.choice(transaction_ids) for x in range(len(ids)-len(transaction_ids))]
dfs['transaction_items']['transaction_id'] = pd.Series(t_ids)
dfs['transaction_items']['quantity'] = dfs['transaction_items']['quantity'].apply(
        lambda x: random.randrange(1,20))


for k,v in dfs.iteritems():

    # wipe out table first
    print 'truncating ' + k
    truncate_query = 'truncate  ' + k + ';'
    db.engine.execute(text(truncate_query).execution_options(autocommit=True))

    # write new records to table
    print 'writing to table ' + k
    v.to_sql(k, db.engine, index=False, if_exists='append')

