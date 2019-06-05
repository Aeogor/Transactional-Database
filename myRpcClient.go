package main

import (
	"ethos/altEthos"
	"ethos/syscall"
	"ethos/kernelTypes"
	"ethos/defined"
	"log"
	"strings"
)

var userName string
var currentTransactionID int64

func init() {
	SetupMyRpcTransactionStartIReply(transactionStartIReply)
	SetupMyRpcTransactionEndIReply(transactionEndIReply)
	SetupMyRpcReadIReply(readIReply)
	SetupMyRpcWriteIReply(writeIReply)
	SetupMyRpcAbortIReply(abortIReply)
}

func transactionStartIReply(transactionID int64, status string) (MyRpcProcedure) {
	//currentTransactionID = transactionID

	if (status == "1") {
		currentTransactionID = transactionID
		printToScreen("Started a new transaction\n")
	}

	return nil
}

func transactionEndIReply(status string) (MyRpcProcedure) {

	if (status != "-1") {
		currentTransactionID = -1
	}

	if(status == "2"){ 
		printToScreen("Nothing to commit for the current transaction\n")
	}

	if(status == "3"){ 
		printToScreen("Committed all changes to the database\n")
	}


	return nil
}

func readIReply(value string, status string) (MyRpcProcedure) {
	
	if (status == "1") {
		printToScreen("Value: ")
		printToScreen(kernelTypes.String(value))
		printToScreen("\n")
	}

	return nil
}

func writeIReply(status string) (MyRpcProcedure) {

	if (status == "1") {
		printToScreen("Updated the value\n")
	}

	return nil
}

func abortIReply(status string) (MyRpcProcedure) {

	if (status == "1") {
		currentTransactionID = -1
		printToScreen("Aborted the transaction\n")
	}
	return nil
}




func sendCall(call defined.Rpc){
	fd, status := altEthos.IpcRepeat("myRpc", "", nil)
	if status != syscall.StatusOk {
		log.Printf("Ipc failed: %v\n", status)
		altEthos.Exit(status)
	}

	status = altEthos.ClientCall(fd, call)
	if status != syscall.StatusOk {
		log.Printf("clientCall failed: %v\n", status)
		altEthos.Exit(status)
	}
}

func startTransaction() {
	call := MyRpcTransactionStartI{}
	sendCall(&call)
}

func endTransaction() {
	if(currentTransactionID == -1) {
		printToScreen("Please start a transaction before any queries\n")
		return
	}

	call := MyRpcTransactionEndI{currentTransactionID}
	sendCall(&call)
}

func readDatabase() {
	if(currentTransactionID == -1) {
		printToScreen("Please start a transaction before any queries\n")
		return
	}

	printToScreen("Enter variable Name: ")
	var variableName kernelTypes.String
	status := altEthos.ReadStream(syscall.Stdin, &variableName)
	if status != syscall.StatusOk {
			log.Printf("Error while reading syscall.Stdin: %v", status)
	}

	call := MyRpcReadI{currentTransactionID, string(strings.TrimRight(string(variableName), "\n"))}
	sendCall(&call)
}

func writeDatabase() {
	if(currentTransactionID == -1) {
		printToScreen("Please start a transaction before any queries\n")
		return
	}

	printToScreen("Enter variable Name: ")
	var variableName kernelTypes.String
	status := altEthos.ReadStream(syscall.Stdin, &variableName)
	if status != syscall.StatusOk {
			log.Printf("Error while reading syscall.Stdin: %v", status)
	}

	printToScreen("Enter variable value: ")
	var variableValue kernelTypes.String
	status = altEthos.ReadStream(syscall.Stdin, &variableValue)
	if status != syscall.StatusOk {
			log.Printf("Error while reading syscall.Stdin: %v", status)
	}

	call := MyRpcWriteI{currentTransactionID, string(strings.TrimRight(string(variableName), "\n")), string(strings.TrimRight(string(variableValue), "\n"))}
	sendCall(&call)
}

func abortTransaction() {
	if(currentTransactionID == -1) {
		printToScreen("Please start a transaction before any queries\n")
		return
	}

	call := MyRpcAbortI{currentTransactionID}
	sendCall(&call)
}

func printToScreen(prompt kernelTypes.String) {  
	statusW := altEthos.WriteStream(syscall.Stdout, &prompt)
	if statusW != syscall.StatusOk {
		log.Printf("Error writing to syscall.Stdout: %v", statusW)
	}
}

func printCommands(){
	printToScreen("\n\nCommands\n")
	printToScreen("---------------------\n")
	printToScreen("-start	: Start a transaction\n")
	printToScreen("-end	: End a transaction\n")
	printToScreen("-read	: Read a varible from the database\n")
	printToScreen("-write	: Write to a variable in the database\n")
	printToScreen("-abort	: Abort a transaction\n")
	printToScreen("---------------------\n\n")

}

func userInputHandler(userInput string) {
	if (userInput == "\n"){
		printCommands()
	} else if (userInput == "??\n") {
		printCommands()
	} else if (userInput == "-start\n"){
		startTransaction()
	} else if (userInput == "-end\n") {
		endTransaction()
	} else if (userInput == "-read\n") {
		readDatabase()
	} else if (userInput == "-write\n") {
		writeDatabase()
	} else if (userInput == "-abort\n") {
		abortTransaction()
	} else {
		printCommands()
	}

}

func getInput(){
	for {
		printToScreen("Enter Input (?? for commands) : ")
		var userInput kernelTypes.String
		status := altEthos.ReadStream(syscall.Stdin, &userInput)
		if status != syscall.StatusOk {
				log.Printf("Error while reading syscall.Stdin: %v", status)
		}

		userInputHandler(string(userInput));
	}
}

func main () {
	altEthos.LogToDirectory("test/myRpcClient")
	
	log.Printf("Database Service: before call\n")

	userName = altEthos.GetUser()

	getInput()

	log.Printf("Database Service: done\n")
}
