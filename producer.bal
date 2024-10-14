import ballerina/io;
import ballerinax/kafka;

type CourseAssignment readonly & record {
    string deadline;
    string mode;
    string presentation;
    };

public function main() returns error? {
    kafka:Producer prod = check new (kafka:DEFAULT_URL);

     while true {
        // Capture user input for the CourseAssignment
        io:print("Enter the deadline (or type 'exit' to quit): ");
        string deadline = io:readln();
        if (deadline == "exit") {
            io:println("Exiting producer...");
            break;
        }
    
    io:println("Welcome to the Kafka tutorial...");

    io:print("Enter the mode (e.g., git-repo): ");
    string mode = io:readln();

    io:print("Enter the presentation title (e.g., DSP): ");
    string presentation = io:readln();


CourseAssignment msg = {
    deadline: deadline,
    mode: mode,
    presentation: presentation
    };

check prod -> send({topic: "dsp", value: msg});

io:println("Message sent successfully to Kafka topic 'dsp': ", msg);}
}