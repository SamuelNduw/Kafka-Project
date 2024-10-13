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
    string customs_clearance_status;
    string status;
    string message;
};

kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, {
    groupId: "international-delivery-service",
    topics: ["international-delivery-requests"]
});

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {});

service on new kafka:Listener(kafkaConsumer) {

    remote function onMessage(kafka:ConsumerRecord[] records) returns error? {
        foreach var record in records {
            ShipmentRequest shipmentRequest = check json.convert(record.value);
            log:printInfo("Processing international delivery request: " + shipmentRequest.toString());

            // Simulate processing and generate a response
            ShipmentResponse response = {
                request_id: shipmentRequest.request_id,
                pickup_time: "2024-10-13T09:00:00Z",
                delivery_time: "2024-10-15T18:00:00Z",
                tracking_number: "INT12345",
                customs_clearance_status: "cleared",
                status: "confirmed",
                message: "International delivery confirmed"
            };

            check kafkaProducer->send({
                topic: "international-delivery-responses",
                value: response.toJsonString()
            });
        }
    }
}
