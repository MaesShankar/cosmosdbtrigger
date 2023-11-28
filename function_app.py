import azure.functions as func
import json
import logging
from azure.cosmos import CosmosClient

# Cosmos DB configuration
cosmosdb_endpoint = "https://roboticscosmos.documents.azure.com:443/"
cosmosdb_key = "sayBm6h6QCxtZn9CVspvKoKLPpflmKqrTiIWUcrZz3uo8s7fq39tVxLt20cwaWhcBMTB2bssrbwqACDbDid1Tw=="
cosmosdb_database_name = "robotics"
cosmosdb_container_name = "temperaturecontainer"

cosmos_client = CosmosClient(cosmosdb_endpoint, cosmosdb_key)
database_client = cosmos_client.get_database_client(cosmosdb_database_name)
container_client = database_client.get_container_client(cosmosdb_container_name)

app = func.FunctionApp()
@app.function_name(name="EventHubTrigger1")
@app.event_hub_message_trigger(arg_name="myhub", 
                               event_hub_name="iothubshank",
                               connection="HostName=iothubshank.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=skPk0Lb+SKFTPvkbxBHH49x1WGuTzoM+JAIoTFywl4c=")

def cosmosdb_trigger(azeventhub: func.EventHubEvent):
    try:
        # Parse the JSON data from the Event Hub event
        json_data = json.loads(azeventhub.get_body().decode('utf-8'))
        
        # Extract relevant fields
        machine_temperature = json_data['machine']['temperature']
        machine_pressure = json_data['machine']['pressure']
        ambient_temperature = json_data['ambient']['temperature']
        ambient_humidity = json_data['ambient']['humidity']
        time_created = json_data['timeCreated']

        # Create a document to be inserted into Cosmos DB
        document = {
            'machine_temperature': machine_temperature,
            'machine_pressure': machine_pressure,
            'ambient_temperature': ambient_temperature,
            'ambient_humidity': ambient_humidity,
            'time_created': time_created
        }

        # Insert the document into Cosmos DB
        container_client.upsert_item(document)

        logging.info('Data inserted into Cosmos DB: %s', document)

    except Exception as e:
        logging.error('Error processing event: %s', str(e))

# Entry point for the Azure Function
if __name__ == "__main__":
    cosmosdb_trigger()