import sys
import json
import requests

from bis import sgcn
from sgcn.sgcn import (addSpeciesToList, processITIS, processWoRMS,
    setupTESSProcessing, processTESS, sgcnDecisions, processNatureServe, synthesize)

include_legacy = False
json_schema = None

def process_1(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
):
    sb_item_id = previous_stage_result

    sbR = requests.get(path).json()
    items = sbR["items"]

    sgcnSourceData = []
    species = []
    for item in items:
        sourceItem = sgcn.sgcn_source_item_metadata(item)
        if sourceItem["processingMetadata"]["sgcn_state"] not in ["Indiana", "Iowa", "Wyoming", "Missouri", "Wisconsin", "New Mexico", "Ohio"]:
            continue

        if sourceItem is None:
            continue
        sourceItemWithData = sgcn.process_sgcn_source_file(sourceItem)
        if not include_legacy:
            duplicates = list(filter(lambda x: 
                x["processingMetadata"]["sgcn_state"] == sourceItemWithData["processingMetadata"]["sgcn_state"] and 
                x["processingMetadata"]["sgcn_year"] == sourceItemWithData["processingMetadata"]["sgcn_year"]
            , sgcnSourceData))
            if len(duplicates):
                dup = duplicates[0]
                if dup["processingMetadata"]["processFileUploadDate"] < sourceItemWithData["processingMetadata"]["processFileUploadDate"]:
                    sgcnSourceData.remove(dup)
                else:
                    continue
        sgcnSourceData.append(sourceItemWithData)
        species = addSpeciesToList(species, sourceItemWithData)

    count = 0
    for index, spec in enumerate(species):
        # if (index < 200
        #     or spec["ScientificName_original"] == "Calypte costae"
        #     or spec["ScientificName_original"] == "Bouteloua gracilis"
        #     or spec["ScientificName_original"] == "Calidris  subruficollis"
        #     or spec["ScientificName_original"] == "Vertigo hubrichti"
        #     or spec["ScientificName_original"] == "Ambystoma laterale"
        #     or spec["ScientificName_original"] == "Ambystoma laterale "):
        send_to_stage(spec, 2)
        count += 1

    return count

def process_2(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
):
    species = previous_stage_result["species"]
    species = processITIS(species)
    species = processWoRMS(species)
    species = setupTESSProcessing(species)
    species = processTESS(species)
    species = sgcnDecisions(species, previous_stage_result)
    species = processNatureServe(species)
    finalSpecies = synthesize(species, previous_stage_result["nsCodes"])

    send_final_result({ "data": finalSpecies, "row_id": finalSpecies["Scientific Name"]})
