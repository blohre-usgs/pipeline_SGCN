import sys
import json
import requests
import copy

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
        # if sourceItem["processingMetadata"]["sgcn_state"] not in ["Indiana", "Iowa", "Wyoming", "Missouri", "Wisconsin", "New Mexico", "Ohio"]:
        #     continue

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
    print('processing ' + species["ScientificName_clean"])
    original_species = copy.deepcopy(species)
    species = processITIS(species)
    itis_species = copy.deepcopy(species)
    print('itis complete')
    species = processWoRMS(species)
    worms_species = copy.deepcopy(species)
    print('worms complete')
    species = setupTESSProcessing(species)
    setup_tess = copy.deepcopy(species)
    print('tess setup complete')
    species = processTESS(species)
    process_tess = copy.deepcopy(species)
    print('tess complete')
    species = sgcnDecisions(species, previous_stage_result)
    sgcn_decisions = copy.deepcopy(species)
    print('decisions complete')
    species = processNatureServe(species)
    process_nature_serve = copy.deepcopy(species)
    print('nature serve complete')
    finalSpecies = synthesize(species, previous_stage_result["nsCodes"])
    print('synthesis complete')

    row_id = finalSpecies["Scientific Name"]
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'processITIS', 'Process ITIS', 'Process ITIS', original_species, itis_species)
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'processWoRMS', 'Process WoRMS', 'Process WoRMS', itis_species, worms_species)
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'setupTESSProcessing', 'Setup TESS Processing', 'Setup TESS Processing', worms_species, setup_tess)
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'processTESS', 'Process TESS', 'Process TESS', setup_tess, process_tess)
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'sgcnDecisions', 'SGCN Decisions', 'SGCN Decisions', process_tess, sgcn_decisions)
    ch_ledger.log_change_event(row_id, 'sgcn.py', 'processNatureServe', 'Process NatureServe', 'Process NatureServe', sgcn_decisions, process_nature_serve)
    ch_ledger.log_change_event(row_id, 'bis_pipeline.py', 'synthesize', 'Synthesize Species Information',
        'Aggregate all of the data gathered for a particular species into one document', process_nature_serve, finalSpecies
    )

    send_final_result({ "data": finalSpecies, "row_id": row_id})
