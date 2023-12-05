import uuid
import azure.functions as func
import json
from azure.cosmos import CosmosClient
import logging

# Cosmos DB configuration
cosmosdb_endpoint = "https://roboticscosmos.documents.azure.com:443/"
cosmosdb_key = "sayBm6h6QCxtZn9CVspvKoKLPpflmKqrTiIWUcrZz3uo8s7fq39tVxLt20cwaWhcBMTB2bssrbwqACDbDid1Tw=="
cosmosdb_database_name = "robotics"
cosmosdb_container_name = "temperaturecontainer"

cosmos_client = CosmosClient(cosmosdb_endpoint, cosmosdb_key)
database_client = cosmos_client.get_database_client(cosmosdb_database_name)
container_client = database_client.get_container_client(cosmosdb_container_name)


app = func.FunctionApp()

@app.function_name(name="iothub-trigger")
@app.event_hub_message_trigger(arg_name="robohub", event_hub_name="iothub-ehub-iothubshan-25363424-574e2f9473",
                               connection="iotHub") 

# def robo_trigger(robohub: func.EventHubEvent):
#     logging.info('Python EventHub trigger processed an event: %s',
#                 robohub.get_body().decode('utf-8')) 



def cosmosdb_trigger(robohub: func.EventHubEvent):
    print("start function")

    try:
        # Parse the JSON data from the Event Hub event
        json_data_str = robohub.get_body().decode('utf-8')
        json_data = json.loads(json_data_str)
        print('Python EventHub trigger processed an event: %s', json_data)
        # Python EventHub trigger processed an event: %s {'machine': {'temperature': 104.53948152349868, 'pressure': 10.51715612293023}, 'ambient': {'temperature': 20.860264751475583, 'humidity': 24}, 'timeCreated': '2023-11-29T09:52:12.8191216Z'}

        # if 'id' not in json_data:
        #     json_data['id'] = str(uuid.uuid4())  # Generate a unique identifier
        # if 'tempid' not in json_data:
        #     json_data['tempid'] = "SimulatedTemp"  # Generate a unique identifier

        # machine_temperature = json_data['machine']['temperature']
        # print(type(machine_temperature))
        # machine_pressure = json_data['machine']['pressure']
        # print(type(machine_pressure))
        # ambient_temperature = json_data['ambient']['temperature']
        # print(type(ambient_temperature))
        # ambient_humidity = json_data['ambient']['humidity']
        # print(type(ambient_humidity))
        # time_created = json_data['timeCreated']
        # print(type(time_created))

        # # Create a document to be inserted into Cosmos DB
        # document = {
        #     'id': json_data['id'],
        #     'tempid': json_data['tempid'],
        #     'machine_temperature': machine_temperature,
        #     'machine_pressure': machine_pressure,
        #     'ambient_temperature': ambient_temperature,
        #     'ambient_humidity': ambient_humidity,
        #     'time_created': time_created
        # }

        # # Insert the document into Cosmos DB
        # container_client.upsert_item(document)

        # print('Data inserted into Cosmos DB: %s', document)

    except Exception as e:
        print('Error processing event: %s', e)

# Entry point for the Azure Function
if __name__ == "__main__":
    cosmosdb_trigger()