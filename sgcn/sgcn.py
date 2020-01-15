from datetime import datetime
import requests
from bis import sgcn, worms, tess
from sgcn import itis, natureserve

def addSpeciesToList(speciesList, sourceItem):
    newSpecies = sourceItem["sourceData"]
    if not len(newSpecies):
        return speciesList
    processingMetadata = sourceItem["processingMetadata"]
    sgcn_year = processingMetadata["sgcn_year"]
    sgcn_state = processingMetadata["sgcn_state"]
    for species in newSpecies:
        sciName = species["scientific name"]
        if not len(sciName):
            continue
        sciNameFound = list(filter(lambda x: x["ScientificName_original"] == sciName, speciesList))
        if len(sciNameFound):
            sgcnDoc = sciNameFound[0]
        else:
            sgcnDoc = sgcn.package_source_name(sciName)
            sgcnDoc["Source Data Summary"] = {"State Submissions":{},"Common Names":[]}
            speciesList.append(sgcnDoc)
        sourceDataSummary = sgcnDoc["Source Data Summary"]
        commonName = species["common name"]
        if commonName.strip() not in sourceDataSummary["Common Names"]:
            sourceDataSummary["Common Names"].append(commonName.strip())
        if sgcn_year not in sourceDataSummary["State Submissions"].keys():
            sourceDataSummary["State Submissions"][sgcn_year] = [sgcn_state]
        else:
            sourceDataSummary["State Submissions"][sgcn_year].append(sgcn_state)

        for year,states in sourceDataSummary["State Submissions"].items():
            sourceDataSummary["State Submissions"][year] = list(set(states))
            sourceDataSummary["State Submissions"][year].sort()
    return speciesList

def processITIS(species):
    itisResult = itis.checkITISSolr(species["ScientificName_clean"])
    if itisResult is not None:
        species["itis"] = itisResult
    return species

def processWoRMS(species):
    if not species["ScientificName_clean"]:
        return species
    wormsResult = worms.lookupWoRMS(species["ScientificName_clean"])
    species["worms"] = wormsResult
    return species

def setupTESSProcessing(species):
    if not species["itis"]["processingMetadata"]:
        return species
    tessDoc = {}
    tessDoc["registration"] = {}
    tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME", species["ScientificName_clean"])

    if "itisData" in species["itis"].keys():
        validITISDoc = next((d for d in species["itis"]["itisData"] if d["usage"] in ["valid", "accepted"]), None)
    else:
        validITISDoc = None

    if "worms" in species.keys():
        if any("status" in d for d in species["worms"]):
            validWoRMSDoc = next((d for d in species["worms"] if d["status"] == "accepted"), None)
        else:
            validWoRMSDoc = None
    else:
        validWoRMSDoc = None

    if validITISDoc is not None:
        tessDoc["registration"]["url_tsn"] = tess.getTESSSearchURL("TSN",validITISDoc["tsn"])
        if species["ScientificName_clean"] != validITISDoc["nameWInd"]:
            tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",validITISDoc["nameWInd"])
    elif validWoRMSDoc is not None:
        tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",validWoRMSDoc["scientificname"])

    species["tess"] = tessDoc
    return species

def processTESS(species):
    processingMetadata = {}
    processingMetadata["matchMethod"] = "Not Matched"
    processingMetadata["dateProcessed"] = datetime.utcnow().isoformat()

    _doName = True
    if "url_tsn" in species["tess"]["registration"].keys():
        tessData = tess.tessQuery(species["tess"]["registration"]["url_tsn"])
        if tessData["result"]:
            processingMetadata["matchMethod"] = "TSN Match"
            _doName = False
    else:
        _doName = True

    if _doName:
        tessData = tess.tessQuery(species["tess"]["registration"]["url_name"])
        if tessData["result"]:
            processingMetadata["matchMethod"] = "SCINAME Match"

    species["tess"]["processingMetadata"] = processingMetadata
    if tessData["result"]:
        species["tess"]["tessData"] = tessData
    return species

def sgcnDecisions(species, previous_stage_result):
    historicSWAPFilePath = previous_stage_result["historicSWAPFilePath"]
    swap2005List = previous_stage_result["swap2005List"]
    sgcnTaxonomicGroupMappings = previous_stage_result["sgcnTaxonomicGroupMappings"]

    for manualMatch in previous_stage_result["itisManualOverrides"]:
        if species["ScientificName_original"] != manualMatch["ScientificName_original"]:
            continue
        itisResult = {}
        itisResult["processingMetadata"] = {}
        itisResult["processingMetadata"]["Date Processed"] = datetime.utcnow().isoformat()
        itisResult["processingMetadata"]["Summary Result"] = "Manual Match"
        url_tsnSearch = manualMatch["taxonomicAuthorityID"]+"&wt=json"
        itisResult["processingMetadata"]["Detailed Results"] = [{"Manual Match":url_tsnSearch}]
        r_tsnSearch = requests.get(url_tsnSearch).json()
        itisResult["itisData"] = [itis.packageITISJSON(r_tsnSearch["response"]["docs"][0])]
        species["itis"] = itisResult

    if not species["itis"] or not species["worms"]:
        return species
    sgcnDoc = {}
    if "itisData" in species["itis"].keys():
        acceptedITISDoc = next((d for d in species["itis"]["itisData"] if d["usage"] in ["accepted","valid"]), None)
        species["Scientific Name"] = acceptedITISDoc["nameWInd"]

        if "commonnames" in acceptedITISDoc.keys():
            cnItem = next((cn for cn in acceptedITISDoc["commonnames"] if cn["language"] == "English"), None)
            if cnItem is not None:
                species["Common Name"] = cnItem["name"]

        species["Match Method"] = species["itis"]["processingMetadata"]["Summary Result"]
        species["Taxonomic Authority Name"] = "ITIS"
        species["Taxonomic Authority ID"] = "https://services.itis.gov/?q=tsn:"+str(acceptedITISDoc["tsn"])
        species["Taxonomic Authority Web URL"] = "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value="+str(acceptedITISDoc["tsn"])
        species["Taxonomic Rank"] = acceptedITISDoc["rank"]
        species["Taxonomy"] = acceptedITISDoc["taxonomy"]
        
    elif "worms" in species.keys():
        acceptedWoRMS = next((doc for doc in species["worms"] if isinstance(doc, str) is False and "status" in doc.keys() and doc["status"] == "accepted"),None)
        if acceptedWoRMS is not None:
            species["Scientific Name"] = acceptedWoRMS["scientificname"]
            species["Taxonomic Authority Name"] = "WoRMS"
            species["Taxonomic Authority ID"] = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"+str(acceptedWoRMS["AphiaID"])
            species["Taxonomic Authority Web URL"] = "http://www.marinespecies.org/aphia.php?p=taxdetails&id="+str(acceptedWoRMS["AphiaID"])
            species["Taxonomic Rank"] = acceptedWoRMS["rank"]
            species["Taxonomy"] = acceptedWoRMS["taxonomy"]
            species["Match Method"] = wormsMatchTypeMapping[acceptedWoRMS["match_type"]]

    if "Scientific Name" not in species.keys():
        species["Scientific Name"] = species["ScientificName_original"]
        species["Match Method"] = "Not Matched"
        species["Taxonomic Authority Name"] = None
        species["Taxonomic Authority ID"] = None
        species["Taxonomic Authority Web URL"] = None
        species["Taxonomic Rank"] = "Undetermined"
        species["Taxonomy"] = None
        species["Taxonomic Group"] = "Other"

        if species["ScientificName_clean"] in swap2005List or species["ScientificName_original"] in swap2005List:
            species["Match Method"] = "Historic Match"
            species["Taxonomic Authority ID"] = historicSWAPFilePath

    if species["Taxonomy"] is not None:
        species["Taxonomic Group"] = sgcn.getTaxGroup(species["Taxonomy"],sgcnTaxonomicGroupMappings)

    if "Common Name" not in species.keys() and len(species["Source Data Summary"]) > 0:
        species["Common Name"] = species["Source Data Summary"]["Common Names"][0]

    return species

def processNatureServe(species):
    natureServePackage = {"processingMetadata":{}}
    natureServePackage["processingMetadata"]["searchName"] = species["Scientific Name"]
    natureServePackage["processingMetadata"]["matchMethod"] = "Not Matched"
    natureServePackage["processingMetadata"]["dateProcessed_search"] = datetime.utcnow().isoformat()
    
    if  len(species["Scientific Name"]) > 0:
        natureServeRecord = natureserve.query.query_natureserve(species["Scientific Name"])
            
        if natureServeRecord is not None:
            natureServePackage["processingMetadata"]["matchMethod"] = "Name Match"
            natureServePackage["NatureServe Record"] = natureServeRecord
    species["NatureServe"] = natureServePackage

    return species

def synthesize(species, nsCodes):
    synthRecord = species
    synthRecord = {
        "_id": species["Scientific Name"],
        "Scientific Name": species["Scientific Name"],
        "Common Name": species["Common Name"],
        "Taxonomic Group": species["Taxonomic Group"],
        "Taxonomic Rank": species["Taxonomic Rank"],
        "Match Method": species["Match Method"],
        "Taxonomic Authority Name": species["Taxonomic Authority Name"],
        "Taxonomic Authority ID": species["Taxonomic Authority ID"],
        "Taxonomic Authority Web URL": species["Taxonomic Authority Web URL"],
        "Taxonomy": species["Taxonomy"],
        "Original Submitted Names":[species["ScientificName_original"]]
    }
    stateSummary = sgcn_state_submissions(species)
    synthRecord["State Summary"] = stateSummary
    tessSummary = sgcn_tess_synthesis(species)
    if tessSummary is not None:
        synthRecord["TESS Summary"] = tessSummary
    synthRecord["NatureServe Summary"] = sgcn_natureserve_summary(species, nsCodes)
    return synthRecord

def sgcn_state_submissions(species):
    states = {"2005":{"included":False,"State List":[],"number":0},"2015":{"included":False,"State List":[],"number":0}}

    try:
        states["2005"]["State List"] = states["2005"]["State List"]+species["Source Data Summary"]["State Submissions"]["2005"]
    except:
        pass
    try:
        states["2015"]["State List"] = states["2015"]["State List"]+species["Source Data Summary"]["State Submissions"]["2015"]
    except:
        pass
    
    if len(states["2005"]["State List"]) > 0:
        states["2005"]["included"] = True
        states["2005"]["State List"] = list(set(states["2005"]["State List"]))
        states["2005"]["State List"].sort()
        states["2005"]["number"] = len(states["2005"]["State List"])

    if len(states["2015"]["State List"]) > 0:
        states["2015"]["included"] = True
        states["2015"]["State List"] = list(set(states["2015"]["State List"]))
        states["2015"]["State List"].sort()
        states["2015"]["number"] = len(states["2015"]["State List"])

    return states

def sgcn_tess_synthesis(species):
    if species["tess"]["processingMetadata"]["matchMethod"] == "Not Matched":
        return None
    tessSynthesis = []

    if len([d for d in tessSynthesis if 'tessData' in species["tess"] and d["ENTITY_ID"] == species["tess"]["tessData"]["ENTITY_ID"]]) == 0:
        if "tessData" in species["tess"]:
            tessSynthesis.append(species["tess"]["tessData"])
            
    if len(tessSynthesis) == 0:
        return None
    else:
        tessRecord = {"Number of TESS Records":len(tessSynthesis),"TESS Records":tessSynthesis}
        tessRecord["Primary Listing Status"] = None
        tessRecord["Primary Listing Date"] = None
        if len(tessSynthesis) == 1:
            try:
                primaryListingStatus = [r for r in tessSynthesis[0]["listingStatus"] if r["POP_DESC"].lower() == "wherever found"]
                if len(primaryListingStatus) == 1:
                    tessRecord["Primary Listing Status"] = primaryListingStatus[0]["STATUS"]
                    if "LISTING_DATE" in primaryListingStatus[0].keys():
                        tessRecord["Primary Listing Date"] = primaryListingStatus[0]["LISTING_DATE"]
                else:
                    tessRecord["Primary Listing Status"] = tessSynthesis[0]["listingStatus"][0]["STATUS"]
                    if "LISTING_DATE" in tessSynthesis[0]["listingStatus"][0].keys():
                        tessRecord["Primary Listing Date"] = tessSynthesis[0]["listingStatus"][0]["LISTING_DATE"]
            except:
                pass

        return tessRecord

def sgcn_natureserve_summary(species, nsCodes):
    if "NatureServe Record" not in species["NatureServe"].keys():
        return {"result":False}
    else:
        nsSummaryRecord = {"result":True,"Date Cached":species["NatureServe"]["processingMetadata"]["dateProcessed_search"]}
        nsSummaryRecord["Element National ID"] = species["NatureServe"]["NatureServe Record"]["@uid"]
        nsSummaryRecord["Element Global ID"] = species["NatureServe"]["NatureServe Record"]["natureServeGlobalConcept"]["@uid"]
        nsSummaryRecord["Rounded National Conservation Status"] = species["NatureServe"]["NatureServe Record"]["roundedNationalConservationStatus"]
        nsSummaryRecord["National Conservation Status Description"] = next((c["definition"] for c in nsCodes if c["code"] == nsSummaryRecord["Rounded National Conservation Status"]), None)
        nsSummaryRecord["Rounded Global Conservation Status"] = species["NatureServe"]["NatureServe Record"]["natureServeGlobalConcept"]["roundedGlobalConservationStatus"]
        nsSummaryRecord["Reference URL"] = species["NatureServe"]["NatureServe Record"]["natureServeGlobalConcept"]["natureServeExplorerURI"]
        try:
            nsSummaryRecord["National Status Last Reviewed"] = species["NatureServe"]["NatureServe Record"]["nationalConservationStatus"]["@lastReviewedDate"]
        except:
            nsSummaryRecord["National Status Last Reviewed"] = None
        try:
            nsSummaryRecord["National Status Last Changed"] = species["NatureServe"]["NatureServe Record"]["nationalConservationStatus"]["@lastChangedDate"]
        except:
            nsSummaryRecord["National Status Last Changed"] = None
        return nsSummaryRecord