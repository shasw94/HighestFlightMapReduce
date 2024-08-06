package main

import (
	"encoding/csv" // Import the encoding/csv package to work with CSV files
	"fmt"          // Import the fmt package for formatted I/O
	"os"           // Import the os package to access file system functions
	"runtime"      // Import the runtime package for information about the Go runtime
	"sync"         // Import the sync package for synchronization primitives
)

// Function to perform the mapping phase
func mapRecord(record []string, output chan<- [2]string) {
	if len(record) != 6 { // Check if the record has 6 fields
		return // Skip if the record does not have 6 fields
	}
	output <- [2]string{record[0], "1"} // Send a key-value pair to the output channel
}

// Function to perform the shuffle phase
func shuffle(input <-chan [2]string) map[string][]string {
	data := make(map[string][]string) // Initialize a map to store shuffled data
	for kv := range input {           // Iterate over key-value pairs from the input channel
		data[kv[0]] = append(data[kv[0]], kv[1]) // Append the value to the key's slice in the map
	}
	return data // Return the shuffled data
}

// Function to perform the reduce phase
func reduce(data map[string][]string, output chan<- [2]string) {
	for k, v := range data { // Iterate over keys and values in the map
		output <- [2]string{k, fmt.Sprintf("%d", len(v))} // Send key-value pair to the output channel with the count of values
	}
}

func main() {
	mapIn := [][]string{} // Initialize a slice to store CSV data

	file, err := os.Open("./AComp_Passenger_data_no_error.csv") // Open the CSV file
	if err != nil {                                             // Check for errors
		panic(err) // Panic if an error occurs
	}
	defer file.Close() // Defer closing the file until the end of the function

	reader := csv.NewReader(file) // Create a new CSV reader
	mapIn, err = reader.ReadAll() // Read all records from the CSV file
	if err != nil {               // Check for errors
		panic(err) // Panic if an error occurs
	}

	numCPU := runtime.NumCPU() // Get the number of CPUs available
	runtime.GOMAXPROCS(numCPU) // Set the maximum number of CPUs to be used by the Go runtime

	mapOut := make(chan [2]string, len(mapIn))    // Create a buffered channel for mapped data
	shuffleIn := make(chan [2]string, len(mapIn)) // Create a buffered channel for shuffled data
	reduceOut := make(chan [2]string, len(mapIn)) // Create a buffered channel for reduced data

	var wg sync.WaitGroup // Initialize a WaitGroup for synchronization

	// Map phase
	for _, record := range mapIn { // Iterate over records in the CSV data
		wg.Add(1)                  // Increment the WaitGroup counter
		go func(record []string) { // Start a goroutine to process each record
			defer wg.Done()           // Decrement the WaitGroup counter when done
			mapRecord(record, mapOut) // Call the mapRecord function to perform mapping
		}(record) // Pass the record to the goroutine
	}

	go func() { // Start a goroutine to wait for all mapping goroutines to finish
		wg.Wait()     // Wait for all goroutines to finish
		close(mapOut) // Close the mapped data channel
	}()

	// Shuffle phase
	go func() { // Start a goroutine to perform the shuffle phase
		for kv := range mapOut { // Iterate over mapped key-value pairs
			shuffleIn <- kv // Send key-value pair to the shuffle channel
		}
		close(shuffleIn) // Close the shuffle channel when done
	}()

	// Reduce phase
	go func() { // Start a goroutine to perform the reduce phase
		defer close(reduceOut)                // Defer closing the reduced data channel until the end of the function
		reduce(shuffle(shuffleIn), reduceOut) // Call the reduce function to perform reduction
	}()

	highFlightNumber := 0               // Initialize a variable to store the highest number of flights
	listHighFlightNumbers := []string{} // Initialize a slice to store passengers with the highest number of flights

	for kv := range reduceOut { // Iterate over reduced key-value pairs
		numFlights := 0                      // Initialize a variable to store the number of flights for each passenger
		fmt.Sscanf(kv[1], "%d", &numFlights) // Parse the number of flights from the value
		if numFlights > highFlightNumber {   // Check if the number of flights is greater than the current highest
			highFlightNumber = numFlights           // Update the highest number of flights
			listHighFlightNumbers = []string{kv[0]} // Clear the list and add the passenger ID
		} else if numFlights == highFlightNumber { // Check if the number of flights is equal to the current highest
			listHighFlightNumbers = append(listHighFlightNumbers, kv[0]) // Add the passenger ID to the list
		}
	}

	fmt.Println("Passengers with the highest number of flights are:") // Print a message
	for _, passenger := range listHighFlightNumbers {                 // Iterate over passengers with the highest number of flights
		fmt.Printf("Passenger ID: %s | Number of flights: %d\n", passenger, highFlightNumber) // Print passenger ID and number of flights
	}
}
