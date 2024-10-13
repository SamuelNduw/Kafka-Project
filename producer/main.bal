import ballerina/log;
import ballerinax/kafka;

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
    string shipment_type;  // standard, express, international
};

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "basic-producer",
    acks: "all",
    retryCount: 3
};

kafka:Producer prod = check new (kafka:DEFAULT_URL, producerConfiguration);


function sendShipmentRequestToKafka(ShipmentRequest shipmentRequest) returns error? {
    // Convert the shipment request to a JSON string
    json shipmentJson = shipmentRequest.toJsonString();

    // Send the shipment request to the "logistics-requests" Kafka topic
    check prod->send({
        topic: "logistics_requests",
        value: shipmentJson.toString()
    });

    log:printInfo("Shipment request sent to logistics service: " + shipmentRequest.toString());
}
public function main() returns error? {

    ShipmentRequest testRequest = {
        request_id: "REQ12345",
        customer_info: {
            first_name: "John",
            last_name: "Doe",
            contact_number: "1234567890"
        },
        shipment_details: {
            pickup_location: "Location A", 
            delivery_location: "Location B", 
            preferred_time_slot: "2024-10-13 10:00:00", 
            shipment_type: "standard"
        }
    };

    checkpanic sendShipmentRequestToKafka(testRequest);

}