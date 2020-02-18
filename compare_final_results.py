import os
import requests
import json
from dotenv import load_dotenv
from deepdiff import DeepDiff

load_dotenv()

get_all = {
    "size": 10,
    "query": {
        "match_all": {}
    },
    "sort": [
        "Scientific Name.keyword"
    ]
}

def get_batch(name, batch_size=100):
    res = get_all
    res["size"] = batch_size
    if name is not None:
        res["search_after"] = [name]
    return res

def get_species_query(name, old=False):
    if old:
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": { "Scientific Name.keyword": name }
                        }
                    ]
                }
            }
        }
    else:
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": { "data.Scientific Name.keyword": name }
                        }
                    ]
                }
            }
        }


    return json.dumps(es_query)

def get_data(data, old=False):
    if old:
        return data['_source']
    return data['_source']['data']

def get_old_species(name=None, batch_size=100):
    # if name is None:
    query = json.dumps(get_batch(name, batch_size))
    # else:
        # query = get_species_query(name)
    url = '{}index={}&q={}'.format(os.environ.get('OLD_ES_HOST'), os.environ.get('OLD_ES_INDEX'), query)
    # print('url' + url)
    val = requests.get(url).json()
    # print(val)
    return val

def get_new_species(name=None):
    if name is None:
        url = '{}{}/_search'.format(os.environ.get('NEW_ES_HOST'), os.environ.get('NEW_ES_INDEX'))
        val = requests.request(method='get',
            headers={'content-type': 'application/json'},
            url=url,
            data=json.dumps(get_all)).json()
        # print(val)
        return val
    else:
        url = '{}{}/_search'.format(os.environ.get('NEW_ES_HOST'), os.environ.get('NEW_ES_INDEX'))
        val = requests.request(method='get',
            headers={'content-type': 'application/json'},
            url=url,
            data=get_species_query(name)).json()
        # print(val)
        return val


# new_res = get_new_species()
# print('new species: ' + str(new_res['hits']['total']))
# print(new_res)
# old_res = get_old_species()
# print('old species: ' + str(old_res['hits']['total']))
# print(old_res)
# compare_file = 'test/result.txt'
# open(compare_file, 'w').close()
# first_species = get_data(new_res['hits']['hits'][0])

# for spec_res in new_res['hits']['hits']:
#     species = get_data(spec_res)
#     old_res = get_old_species(species['Scientific Name'])
#     old = get_data(old_res['hits']['hits'][0], True)

#     ddiff = DeepDiff(old, species)
#     with open(compare_file, 'a') as res:
#         res.write(str(ddiff) + "\n")

batch_size = 100
old_res = get_old_species(batch_size=batch_size)
total_checked = 0
total = old_res["hits"]["total"]
while total_checked < total:
    for spec_res in old_res['hits']['hits']:
        species = get_data(spec_res, True)
        sci_name = species['Scientific Name']
        new_hit = get_new_species(sci_name)
        # print(species['Scientific Name'] + ': ' + str(new_hit))
        hits = new_hit['hits']['hits']
        if len(hits):
            new_species = get_data(hits[0])
            # print(sci_name + ' n: ' + new_species['Scientific Name'])
        else:
            print('NO HIT: ' + sci_name)
        total_checked += 1
        last_name = sci_name
    if total_checked < total:
        print('new batch starting, total: ' + str(total_checked))
        old_res = get_old_species(last_name, batch_size)
