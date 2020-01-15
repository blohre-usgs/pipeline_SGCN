import sys
import json
import requests

from bis import sgcn
from sgcn.sgcn import (addSpeciesToList, processITIS, processWoRMS,
    setupTESSProcessing, processTESS, sgcnDecisions, processNatureServe, synthesize)

include_legacy = False

def process_1(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
):
    sb_item_id = previous_stage_result
    sbSWAPItem = requests.get("https://www.sciencebase.gov/catalog/item/" + sb_item_id + "?format=json&fields=files").json()
    for file in sbSWAPItem["files"]:
        if file["title"] == "Historic 2005 SWAP National List":
            _historicSWAPFilePath = file["url"]
            swap2005List = []
            for line in requests.get(file["url"], stream=True).iter_lines():
                if line: swap2005List.append(line.decode("utf-8"))
        elif file["title"] == "Taxonomic Group Mappings":
            sgcnTaxonomicGroupMappings = json.loads(requests.get(file["url"]).text)
        elif file["title"] == "SGCN ITIS Overrides":
            itisManualOverrides = json.loads(requests.get(file["url"]).text)
        elif file["title"] == "NatureServe National Conservation Status Descriptions":
            nsCodes = json.loads(requests.get(file["url"]).text)

    sbR = requests.get(path).json()
    items = sbR["items"]

    sgcnSourceData = []
    species = []
    for item in items:
        sourceItem = sgcn.sgcn_source_item_metadata(item)
        if sourceItem["processingMetadata"]["sgcn_state"] not in ["Indiana", "Iowa", "Wyoming", "Missouri", "Wisconsin", "New Mexico"]:
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
        # for species in sourceItemWithData["sourceData"]:
        #     name = species["scientific name"]
        #     if len(name) > 0 and name not in uniqueNames:
        #         uniqueNames.append(name)

    # with open('test.txt', 'w') as testFile:
    #     for sourceData in sgcnSourceData:
    #         testFile.write(str(sourceData))
    #         testFile.write('\n')
    # with open('species.txt', 'w') as speciesFile:
    #     for sp in species:
    #         if sp["ScientificName_original"] == "Thamnophis radix":
    #             speciesFile.write(str(sp))
    # s = sb_io.SbIo(sb_item_id, sb_file_name, run_id)
    # path = s.download_and_extract_file()
        # if index == 0:
        #     print(sourceItemWithData)
    # ch_ledger = change_ledger.ChangeLedger(run_id, "USNVC", "usnvc_1.py", sb_item_id, sb_file_name)
    # print('Number of species: ' + str(len(uniqueNames)))

    for index, spec in enumerate(species):
        if (index < 5
            or spec["ScientificName_original"] == "Calypte costae"
            or spec["ScientificName_original"] == "Bouteloua gracilis"
            or spec["ScientificName_original"] == "Calidris  subruficollis"
            or spec["ScientificName_original"] == "Vertigo hubrichti"):
            send_to_stage({
                "species": spec,
                "historicSWAPFilePath": _historicSWAPFilePath,
                "swap2005List": swap2005List,
                "sgcnTaxonomicGroupMappings": sgcnTaxonomicGroupMappings,
                "itisManualOverrides": itisManualOverrides,
                "nsCodes": nsCodes
            }, 2)

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

    # with open('test/species.json', 'a') as speciesFile:
    #     species["_id"] = species["ScientificName_original"]
    #     speciesFile.write(json.dumps(species) + '\n')
    # with open('test/final_species.json', 'a') as finalSpeciesFile:
    #     finalSpeciesFile.write(json.dumps(finalSpecies) + "\n")
