import ballerinax/kafka;
import ballerina/log;

type ShipmentRequest record {
    string request_id;
    CustomerInfo customer_info;
    ShipmentDetails shipment_details;
};

type CustomerInfo record {
    string first_name;
    string last_name;
    string contact_number;
};

type ShipmentDetails record {
    string pickup_location;
    string delivery_location;
    string preferred_time_slot;
    string shipment_type;
};

type ShipmentResponse record {
    string request_id;
    string pickup_time;
    string delivery_time;
    string tracking_number;
    string status;
    string message;
};

kafka:ConsumerConfiguration kafkaConsumer = {
    groupId: "logistics-service",
    topics: ["logistics_requests"]
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {});

service on new kafka:Listener(kafka:DEFAULT_URL, kafkaConsumer) {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            byte[] messageContent = consumerRecord.value;
            string result = check string:fromBytes(messageContent);
            json shipmentRequestJSON = check result.fromJsonString();
            ShipmentRequest shipmentRequest = check result.fromJsonStringWithType();
            log:printInfo("Received request: " + shipmentRequest.toString());

            // Route based on shipment type
            if shipmentRequest.shipment_details.shipment_type == "standard" {
                check kafkaProducer->send({
                    topic: "standard_delivery_requests",
                    value: shipmentRequest.toJsonString()
                });
            } else if shipmentRequest.shipment_details.shipment_type == "express" {
                check kafkaProducer->send({
                    topic: "express-delivery-requests",
                    value: shipmentRequest.toJsonString()
                });
            } else if shipmentRequest.shipment_details.shipment_type == "international" {
                check kafkaProducer->send({
                    topic: "international-delivery-requests",
                    value: shipmentRequest.toJsonString()
                });
            }
        }
    }
}
