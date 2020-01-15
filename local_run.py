from sgcn import bis_pipeline

path = './SGCN_Source_Data.json'
ch_ledger = 'ledger'
send_final_result = 'send_final_result'
previous_stage_result = 'previous_stage_result'

def send_to_stage(data, stage):
    bis_pipeline.process_2(path, ch_ledger, send_final_result, send_to_stage, data)

def lambda_handler(event, context):
    run_id = event["run_id"]
    sb_item_id = event["sb_item_id"]
    download_uri = event["download_uri"]

    bis_pipeline.process_1(download_uri, ch_ledger, send_final_result, send_to_stage, sb_item_id)

# open('test/species.json', 'w').close()
# open('test/final_species.json', 'w').close()

lambda_handler({
    "run_id": 1,
    "sb_item_id": "56d720ece4b015c306f442d5",
    "download_uri": "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=1000"
}, {})