import requests
import json
from sgcn import bis_pipeline

species_file = 'test/species.json'
final_species_file = 'test/final_species.json'
ch_ledger = 'ledger'

def download_and_extract(file_url, run_id, file_name = None):
    path = "test/" + run_id + "_" + file_name
    try:
        open(path).read() # see if the file already exists
    except:
        local_file = requests.get(file_url)
        open(path, "wb").write(local_file.content)
    return path

def lambda_handler_2(event, context):
    message_in = json.loads(event["body"])
    run_id = message_in["run_id"]
    sb_item_id = message_in["sb_item_id"]
    download_uri = message_in["download_uri"]

    sbSWAPItem = requests.get("https://www.sciencebase.gov/catalog/item/" + sb_item_id + "?format=json&fields=files").json()


    for file in sbSWAPItem["files"]:
        if file["title"] == "Historic 2005 SWAP National List":
            _historicSWAPFilePath = file["url"]
            swap2005List = []
            path = download_and_extract(file["url"], run_id, "Historic 2005 SWAP National List")
            with open(path, "rb") as hsnl:
                for line in hsnl:
                    if line: swap2005List.append(line.decode("utf-8"))
        elif file["title"] == "Taxonomic Group Mappings":
            path2 = download_and_extract(file["url"], run_id, "Taxonomic Group Mappings")
            with open(path2, "r") as tgm:
                sgcnTaxonomicGroupMappings = json.load(tgm)
        elif file["title"] == "SGCN ITIS Overrides":
            path3 = download_and_extract(file["url"], run_id, "SGCN ITIS Overrides")
            with open(path3, "r") as sio:
                itisManualOverrides = json.load(sio)
        elif file["title"] == "NatureServe National Conservation Status Descriptions":
            path4 = download_and_extract(file["url"], run_id, "NatureServe National Conservation Status Descriptions")
            with open(path4, "r") as nncsd:
                nsCodes = json.load(nncsd)

    data = {
        "species": message_in["payload"],
        "historicSWAPFilePath": _historicSWAPFilePath,
        "swap2005List": swap2005List,
        "sgcnTaxonomicGroupMappings": sgcnTaxonomicGroupMappings,
        "itisManualOverrides": itisManualOverrides,
        "nsCodes": nsCodes
    }

    send_to_stage = None
    def send_final_result(data):
        species = data["data"]
        row_id = data["row_id"]
        with open(final_species_file, 'a') as finalSpeciesFile:
            species["_id"] = row_id
            finalSpeciesFile.write(json.dumps(species) + "\n")

    bis_pipeline.process_2(path, ch_ledger, send_final_result, send_to_stage, data)


def lambda_handler(event, context):
    run_id = event["run_id"]
    sb_item_id = event["sb_item_id"]
    download_uri = event["download_uri"]

    def send_to_stage(data, stage):
        with open(species_file, 'a') as speciesFile:
            data["_id"] = data["ScientificName_original"]
            speciesFile.write(json.dumps(data) + '\n')
        json_doc = {
            'run_id': run_id,
            'sb_item_id': sb_item_id,
            'download_uri': download_uri,
            'payload': data
        }
        lambda_handler_2({"body": json.dumps(json_doc)}, {})

    send_final_result = None

    num_species = bis_pipeline.process_1(download_uri, ch_ledger, send_final_result, send_to_stage, sb_item_id)
    print("Processing species count: " + str(num_species))

open(species_file, 'w').close()
open(final_species_file, 'w').close()

lambda_handler({
    "run_id": "1",
    "sb_item_id": "56d720ece4b015c306f442d5",
    "download_uri": "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=1000"
}, {})

# lambda_handler_2({"body": json.dumps({
#     "run_id": "1",
#     "sb_item_id": "56d720ece4b015c306f442d5",
#     "download_uri": "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=1000",
#     "payload": {}
# })}, {})