# influxdb-kafka-demo

This repo contains an example for how to use Kafka, Telegraf, and InfluxDB Cloud v3 together to monitor a garden. 

## Run
To run this example repo follow these steps: 
1. Add your InfluxDB Cloud v3 url, token, org, and bucket to lines 7,9,11,and 14 respectively of `resources/mytelegraf.conf`
2. Run the containers with `docker-compose up --build`
3. Wait 30 seconds before Telegraf is ready to write metrics

## Files
- `app/garden_sensor_gateway.py`: is a Python Script uses the KafkaProducer class from the kafka package to send generated garden sensor data to a Kafka topic. It includes random humidity, temperature, wind, and soil data. 
- `app/Dockerfile`: creates a container that runs `garden_sensor_gateway.py`
- `resources/docker-compose`: creates the containers the kafka, zookeeper, telegraf and garden_sensor_gateway containers
- `resources/mytelegraf.conf`: contains the telegraf configuration to subscribe to the kafka topic and write the garden data to InfluxDB Cloud v3. 
