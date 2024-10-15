import ballerina/http;
import ballerinax/kafka;
import ballerina/log;

listener http:Listener httpListener = new(8080);

// Kafka producer configuration
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {
    groupId: "logistics_group",
    topics: ["standard", "express", "international"]
});

service /logistics on httpListener {

    resource function post request(http:Caller caller, http:Request req) returns error? {
        json requestData = check req.getJsonPayload();
        string shipmentType = requestData.shipmentType.toString();

        string topic;
        if shipmentType == "standard" {
            topic = "standard";
        } else if shipmentType == "express" {
            topic = "express";
        } else if shipmentType == "international" {
            topic = "international";
        } else {
            check caller->respond({ message: "Invalid shipment type" });
            return;
        }

        // Publish the request to the appropriate Kafka topic
        kafka:ProducerRecord record = { value: requestData.toString() };
        check kafkaProducer->send({ topic: topic, value: record.value });

        log:printInfo("Sent request to " + topic + " topic: " + requestData.toString());
        check caller->respond({ message: "Request sent to " + shipmentType + " service" });
    }
}

