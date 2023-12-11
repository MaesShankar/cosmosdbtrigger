import uuid
import azure.functions as func
import json
from azure.cosmos import CosmosClient
import logging

# Cosmos DB configuration
cosmosdb_endpoint = "https://roboticscosmos.documents.azure.com:443/"
cosmosdb_key = "sayBm6h6QCxtZn9CVspvKoKLPpflmKqrTiIWUcrZz3uo8s7fq39tVxLt20cwaWhcBMTB2bssrbwqACDbDid1Tw=="
cosmosdb_database_name = "robotics"
cosmosdb_container_name = "PLC_Data"

cosmos_client = CosmosClient(cosmosdb_endpoint, cosmosdb_key)
database_client = cosmos_client.get_database_client(cosmosdb_database_name)
container_client = database_client.get_container_client(cosmosdb_container_name)

app = func.FunctionApp()

@app.function_name(name="iothub-trigger")
@app.event_hub_message_trigger(arg_name="robohub", event_hub_name="iothub-ehub-shankarhub-25384865-eae4bd5211",
                               connection="iotHub") 

# @app.event_hub_message_trigger(arg_name="robohub", event_hub_name="iothub-ehub-iothub-rob-25330811-5c50f3e599",
#                                connection="iotHub")
def cosmosdb_trigger(robohub: func.EventHubEvent):

    try:
        # Parse the JSON data from the Event Hub event
        json_data_str = robohub.get_body().decode('utf-8')
        json_data = json.loads(json_data_str)
        print('Trigger Received: %s', json_data)
        
        if 'id' not in json_data:
            json_data['id'] = str(uuid.uuid4())  # Generate a unique identifier

        # Check the DeviceId to determine which data structure to use
        if json_data['DeviceId'] == 'WeatherStationModbus':
            print("WeatherStationModbus")
            # Handle WeatherStationModbus data
            document = {
                "id": json_data['id'],
                'DeviceId': json_data['DeviceId'],
                'brightness': json_data['brightness'],
                'windspeed': json_data['windspeed'],
                'temperature': json_data['temperature'],
                'humidity': json_data['humidity'],
            }
        elif json_data['DeviceId'] == 'UnilogicPLC_OPC':
            print("UnilogicPLC_OPC")
            # Handle UnilogicPLC_OPC data
            document = {
                "id": json_data['id'],
                'DeviceId': json_data['DeviceId'],
                'temperature': json_data['Temperature'],
            }
        elif json_data['DeviceId'] == 'EnergyMonitoring':
            print("EnergyMonitoring")
            # Handle EnergyMonitoring data
            document = {
                "id": json_data['id'],
                'DeviceId': json_data['DeviceId'],
                'voltages': json_data['voltages'],
                'currents': json_data['currents'],
                'active_power': [abs(value) for value in json_data['active_power']],
            }
        elif json_data['DeviceId'] == 'LORA':
            print("LORA")
            # Handle LORA data
            document = json_data
        else:
            print(f"Unknown DeviceId: {json_data['DeviceId']}")
                        
        
        # Insert the document into Cosmos DB
        container_client = database_client.get_container_client(cosmosdb_container_name)
        container_client.upsert_item(document)
        print(f"Data inserted into Cosmos DB for {json_data['DeviceId']}: {document}")
        

    except Exception as e:
        print('Error processing event: %s', e)

# Entry point for the Azure Function
if __name__ == "__main__":
    cosmosdb_trigger()