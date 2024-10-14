import ballerinax/kafka;
import ballerina/log;
import ballerina/sql;
import ballerinax/mysql;
import ballerina/os;

configurable string dbPassword = os:getEnv("DB_PASSWORD");

mysql:Client dbClient = check new (
    host = "localhost",
    port = 3306,
    database = "logisticsDB",
    user = "root",
    password = "password123"
);


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

type Customer record {
    int id;
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
    groupId: "standard-delivery-service",
    topics: ["standard_delivery_requests"]
};

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {});

function addCustomer(CustomerInfo customerInfo) returns int|error {
    sql:ParameterizedQuery query = `INSERT INTO customers (first_name, last_name, contact_number) VALUES (${customerInfo.first_name}, ${customerInfo.last_name}, ${customerInfo.contact_number})`;
    sql:ExecutionResult result = check dbClient->execute(query);
    log:printInfo("Customer added successfully: " + customerInfo.first_name);
    sql:ParameterizedQuery query2 = `CALL GetLastCustomer()`;
    stream<Customer, sql:Error?> resultStream = dbClient->query(query2);
    Customer[] customerArray = [];
    error? customerErr = resultStream.forEach(function(Customer customer){
        customerArray.push(customer);
    });

    int customerId = 0;
    if(customerErr is error) {
        log:printInfo("Error while trying to get Customer information");
    }else{
        customerId = customerArray[0].id;
    }

    return customerId;
}

// Insert shipment record into the `shipments` table
function addShipment(ShipmentRequest shipmentRequest, ShipmentDetails shipmentDetails, int customerId, ShipmentResponse shipmentResponse) returns error? {
    sql:ParameterizedQuery query = `INSERT INTO shipments (request_id, customer_id, pickup_location, delivery_location, preferred_time_slot, shipment_type, tracking_number, status, pickup_time, delivery_time) VALUES (${shipmentRequest.request_id}, ${customerId}, ${shipmentDetails.pickup_location}, ${shipmentDetails.delivery_location},
                            ${shipmentDetails.preferred_time_slot}, ${shipmentDetails.shipment_type}, ${shipmentResponse.tracking_number},
                            ${shipmentResponse.status}, ${shipmentResponse.pickup_time}, ${shipmentResponse.delivery_time})`;
    _ = check dbClient->execute(query);
    log:printInfo("Shipment added successfully for request_id: " + shipmentRequest.request_id);
}

service on new kafka:Listener(kafka:DEFAULT_URL, kafkaConsumer) {

    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach var consumerRecord in records {
            byte[] messageContent = consumerRecord.value;
            string result = check string:fromBytes(messageContent);
            json shipmentRequestJSON = check result.fromJsonString();
            ShipmentRequest shipmentRequest = check result.fromJsonStringWithType();
            ShipmentDetails shipmentDetails =  shipmentRequest.shipment_details;
            CustomerInfo customerInfo = shipmentRequest.customer_info;
            log:printInfo("Processing standard delivery request: " + customerInfo.first_name);

            int customerId = check addCustomer(customerInfo);

            // Simulate processing and generate a response
            ShipmentResponse response = {
                request_id: shipmentRequest.request_id,
                pickup_time: "2024-10-12 10:00:00",
                delivery_time: "2024-10-13 15:00:00",
                tracking_number: "STD12345",
                status: "confirmed",
                message: "Standard delivery confirmed"
            };

            check addShipment(shipmentRequest, shipmentDetails, customerId, response);

            check kafkaProducer->send({
                topic: "standard_delivery_responses",
                value: response.toJsonString()
            });
        }
    }
}
