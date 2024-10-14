import ballerina/http;
import ballerinax/kafka;

// Create a Kafka producer
kafka:Producer kafkaProducer = new({
    bootstrapServers: "localhost:9092"
});

service /logistics on new http:Listener(9090) {

    resource function post schedulePickup(http:Caller caller, http:Request req) returns error? {
        // Parse the customer request
        json payload = check req.getJsonPayload();
        string shipmentType = payload.type.toString();

        // Send to Kafka based on shipment type
        if (shipmentType == "standard") {
            check kafkaProducer->send({ topic: "standard-delivery", value: payload.toJsonString() });
        } else if (shipmentType == "express") {
            check kafkaProducer->send({ topic: "express-delivery", value: payload.toJsonString() });
        } else if (shipmentType == "international") {
            check kafkaProducer->send({ topic: "international-delivery", value: payload.toJsonString() });
        }

        // Respond to customer
        json response = { "message": "Request received", "trackingId": "<TRACKING_ID>" };
        check caller->respond(response);
    }
}
