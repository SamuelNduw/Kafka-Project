import ballerinax/kafka;
import ballerina/log;

type ShipmentRequest record {
    string request_id;
    CustomerInfo customer_info;
    ShipmentDetails shipment_details;
};

type ShipmentResponse record {
    string request_id;
    string pickup_time;
    string delivery_time;
    string tracking_number;
    string status;
    string message;
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, {
    groupId: "express-delivery-service",
    topics: ["express-delivery-requests"]
});

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {});

service on new kafka:Listener(kafkaConsumer) {

    remote function onMessage(kafka:ConsumerRecord[] records) returns error? {
        foreach var record in records {
            ShipmentRequest shipmentRequest = check json.convert(record.value);
            log:printInfo("Processing express delivery request: " + shipmentRequest.toString());

            // Simulate processing and generate a response
            ShipmentResponse response = {
                request_id: shipmentRequest.request_id,
                pickup_time: "2024-10-12T08:00:00Z",
                delivery_time: "2024-10-12T12:00:00Z",
                tracking_number: "EXP12345",
                status: "confirmed",
                message: "Express delivery confirmed"
            };

            check kafkaProducer->send({
                topic: "express-delivery-responses",
                value: response.toJsonString()
            });
        }
    }
}
