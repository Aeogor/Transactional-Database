package main

import (
	"ethos/syscall"
	"ethos/altEthos"
	"ethos/kernelTypes"
	"log"
)


type queueStruct struct {
	_type string
	FDValue syscall.Fd
	transactionID int64
	variableName string
	variableValue string
}


var path = "/user/" + altEthos.GetUser() + "/server/"
var pathTypeServer kernelTypes.String
var logType Databaselog
var storeType kernelTypes.String
var numTrasactions int64 = 1
var currentEventID syscall.EventId


var readLocks = make(map[string][]int64)
var writeLocks = make(map[string][]int64)
var event_fd = make(map[syscall.EventId]syscall.Fd)
var queue = make([]queueStruct, 0)
var transactionCommits = map[int64]map[string]string{}


func init() {
	SetupMyRpcTransactionStartI(transactionStartI)
	SetupMyRpcTransactionEndI(transactionEndI)
	SetupMyRpcReadI(readI)
	SetupMyRpcWriteI(writeI)
	SetupMyRpcAbortI(abortI)
}

func transactionStartI() (MyRpcProcedure) {
	status := "1"
	transactionID := numTrasactions
	numTrasactions++

	transactionCommits[transactionID] = map[string]string{}


	return &MyRpcTransactionStartIReply{transactionID, status}
}

func transactionEndI(id int64) (MyRpcProcedure) {
	status := "1"
	
	removeTransactionFromReadLocks(id)
	removeTransactionFromWriteLocks(id)

	tmap := transactionCommits[id]
	if tmap == nil {
		status = "-1"
		log.Printf("Transaction doesnt exist\n")
		processQueue()
		return &MyRpcTransactionEndIReply{status} 
	}

	if len(tmap) == 0 {
		transactionCommits[id] = tmap, false
		status = "2"
		log.Printf("Nothing to commit, returning\n")
		processQueue()
		return &MyRpcTransactionEndIReply{status}
	}

	for k, v := range tmap { 
		writeToFile(k,string(v))
		log.Printf("Writing to file\n")
	}

	transactionCommits[id] = tmap, false
	processQueue()

	status = "3"
	return &MyRpcTransactionEndIReply{status}
}

func writeToFile (variableName string, value string) {

	writeToLog(variableName, value)

	log.Println("Writing to store")
	writeToStore := true

	fd, status := altEthos.DirectoryOpen(path + "store/")
	if status != syscall.StatusOk {
		log.Fatalf ("Error opening %v: %v\n", path, status)
		writeToStore = false
	}

	var datatypeString kernelTypes.String 
	datatypeString = kernelTypes.String(value)

	status = altEthos.WriteVar(fd, variableName, &datatypeString)
	if status != syscall.StatusOk {
	   log.Printf ("Error Writing to %v %v\n", path + "/" + variableName , status)
	   writeToStore = false
	}   

	if writeToStore == true {
		deleteFromLog()
	}
}


func writeToLog(variableName string, variableValue string) {
	log.Println("Writing to log")
	logStruct := Databaselog{variableName, variableValue}

	fd, status := altEthos.DirectoryOpen(path + "log/")
	if status != syscall.StatusOk {
		log.Fatalf ("Error opening %v: %v\n", path, status)
	}

	status = altEthos.WriteStream(fd, &logStruct)
	if status != syscall.StatusOk {
	   log.Printf ("Error Writing to %v %v\n", path + "/" + variableName , status)
	}
}

func deleteFromLog() {
	log.Println("Deleting from log")
	FileNames, status := altEthos.SubFiles(path + "log/")
	if status != syscall.StatusOk {
		log.Fatalf("Error fetching files in %v\n", path)
	}

	i := len(FileNames) - 1
	altEthos.FileRemove(path + "log/" + FileNames[i])
	if status != syscall.StatusOk {
		log.Fatalf("Error deleting files in %v\n", path)
	}
}


func readI(id int64, variableName string) (MyRpcProcedure) {

	checkRead := checkIfTransactionContainsReadLockForVar(id, variableName)
	checkWrite := checkIfTransactionContainsWriteLockForVar(id, variableName)

	//if the transaction contains either a read lock or a write lock
	//Then it should be able to read the value
	if(checkWrite == true){
		value := transactionCommits[id][variableName]
		log.Printf("i obtain the write lock so getting the value from there\n")
		return &MyRpcReadIReply{ value, "1"} 
	}

	if(checkRead == true){
		value := readFromFile(path, variableName)
		if(value == ""){
			return &MyRpcReadIReply{ "-1", "Variable Doesn't Exist"} 
		}
		log.Printf("I obtain the read lock, so getting the value from file\n")
		return &MyRpcReadIReply{ value, "1"} 
	}

	//Make sure no one else has a write lock
	i := writeLocks[variableName]
	currentFD := event_fd[currentEventID]
	if len(i) == 0 {
		//grab the read lock
		readLocks[variableName] = append(readLocks[variableName] , id)

		value := readFromFile(path, variableName)
		if(value == ""){
			return &MyRpcReadIReply{ "", "Variable Doesn't Exist"} 
		}
		
		log.Printf("Obtained a new read lock\n")
		return &MyRpcReadIReply{ value, "1"} 

	}
	
	//add the query to the queue and return  nil
	newReadQ := queueStruct{"read", currentFD, id, variableName, ""}
	queue = append(queue, newReadQ)
	log.Printf("Some one else has a write lock for this variable\n")
	return nil


	
}

func readFromFile(path string, filename string) (string){
	_, status1 := altEthos.DirectoryOpen(path + "store/")
	if status1 != syscall.StatusOk {
		log.Println("Directory Create Failed ", path, status1)
		return ""
	}

	var value kernelTypes.String
	status1 = altEthos.Read(path + "store/" + filename, &value)
	if status1 != syscall.StatusOk {
		log.Fatalf("Error reading box file at %v/%v\n", path + "store/" + filename, filename)
		return ""
	}

	return string(value)

}

func writeI(id int64, val string, value string) (MyRpcProcedure) {

	log.Printf("Called write transaction\n")

	checkWrite := checkIfTransactionContainsWriteLockForVar(id, val)
	if(checkWrite == true){ 
		//if the transaction already contains a write lock
		transactionCommits[id][val] = value
		return &MyRpcWriteIReply{"1"}
	}

	currentFD := event_fd[currentEventID]

	i := writeLocks[val]
	if len(i) > 0 {
		//Someone else holds the write lock
		newWriteQ := queueStruct{"write", currentFD, id, val, value}
		queue = append(queue, newWriteQ)
		log.Printf("There are write locks for this variable\n")
		return nil

	} else if len(i) == 0 {
		i = readLocks[val]
		checkRead := checkIfTransactionContainsReadLockForVar(id, val)

		if len(i) == 0 {
			//Obtain a write lock
			writeLocks[val] = append(writeLocks[val] , id)
			transactionCommits[id][val] = value
			log.Printf("No read locks, obtaining a write lock\n")
			return &MyRpcWriteIReply{"1"}
		}

		if (checkRead == true && (len(i) == 1)){
			//If I am the only one who contains the read lock
			upgradeReadLocktoWriteLock(id, val)
			transactionCommits[id][val] = value
			log.Printf("The user contains the only read lock\n")
			return &MyRpcWriteIReply{"1"}
		} 
		
		// if (checkRead == false || (len(i) > 1)){
		// 	//Cant obtain a write lock
		// 	newWriteQ := queueStruct{"write", currentFD, id, val, value}
		// 	queue = append(queue, newWriteQ)
		// 	return &MyRpcWriteIReply{nil}
		// } 
		
	}
	//Cant obtain a write lock
	newWriteQ := queueStruct{"write", currentFD, id, val, value}
	queue = append(queue, newWriteQ)
	log.Printf("Cannot obtain a lock so waiting\n")
	return nil
}

func upgradeReadLocktoWriteLock(id int64, val string) {
	v := readLocks[val]
	for i, a := range v {
		if a == id {
			readLocks[val] = removeIDFromSlice(v, i)
			break
		}
	}

	writeLocks[val] = append(writeLocks[val] , id)
}

func abortI(id int64) (MyRpcProcedure) {
	//var status string

	removeTransactionFromReadLocks(id)
	removeTransactionFromWriteLocks(id)
	transactionCommits[id] = transactionCommits[id], false
	processQueue()
	
	return &MyRpcAbortIReply{"1"}
}

func removeTransactionFromReadLocks(id int64) {
	for k, v := range readLocks { 
		for i, a := range v {
			if a == id {
				readLocks[k] = removeIDFromSlice(v, i)
				break
			}
		}
	}
}

func removeTransactionFromWriteLocks(id int64) {
	for k, v := range writeLocks { 
		for i, a := range v {
			if a == id {
				writeLocks[k] = removeIDFromSlice(v, i)
				break
			}
		}
	}
}

func checkIfTransactionContainsReadLockForVar(id int64, _var string) bool {
	v := readLocks[_var]
	for _, a := range v {
		if a == id {
			return true
		}
	}
	return false	
}

func checkIfTransactionContainsWriteLockForVar(id int64, _var string) bool {
	v := writeLocks[_var]
	for _, a := range v {
		if a == id {
			return true
		}
	}
	return false	
}

func removeIDFromSlice(s []int64, i int) []int64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func removeStructFromSlice(s []queueStruct, i int) []queueStruct{
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func sendAnRPCReadReply(FDValue syscall.Fd, value string, status string) () {
	status1 := altEthos.WriteStream(FDValue, &MyRpcReadIReply{value, status})
	if status1 != syscall.StatusOk {
		log.Fatalf("Error returning status\n")
	}
}

func sendAnRPCWriteReply(FDValue syscall.Fd, status string) () {
	status1 := altEthos.WriteStream(FDValue, &MyRpcWriteIReply{status})
	if status1 != syscall.StatusOk {
		log.Fatalf("Error returning status\n")
	}
}

func processQueue() {
	for _, q := range queue {
		if (q._type == "read"){
			i := writeLocks[q.variableName]
			if len(i) == 0 {
				checkRead := checkIfTransactionContainsReadLockForVar(q.transactionID, q.variableName)
				if (checkRead == false){
					readLocks[q.variableName] = append(readLocks[q.variableName] , q.transactionID)
				}
				q._type = "finished"

				value := readFromFile(path, q.variableName)
				if(value == ""){
					sendAnRPCReadReply(q.FDValue, "-1","-1")
				}
				sendAnRPCReadReply(q.FDValue, value, "1")
			} else {
				continue
			}
		} else if (q._type == "write") {
			//if the transaction holds the write lock
			checkWrite := checkIfTransactionContainsWriteLockForVar(q.transactionID, q.variableName)
			if(checkWrite == true){ 
				//if the transaction already contains a write lock
				transactionCommits[q.transactionID][q.variableName] = q.variableValue
				sendAnRPCWriteReply(q.FDValue, "1")

			}

			i := writeLocks[q.variableName]
			if len(i) == 0 {
				i = readLocks[q.variableName]
				checkRead := checkIfTransactionContainsReadLockForVar(q.transactionID, q.variableName)

				if len(i) == 0 {
					//Obtain a write lock
					writeLocks[q.variableName] = append(writeLocks[q.variableName] , q.transactionID)
					transactionCommits[q.transactionID][q.variableName] = q.variableValue
					q._type = "finished"
					sendAnRPCWriteReply(q.FDValue , "1")

				}

				if (checkRead == true && (len(i) == 1)){
					//If I am the only one who contains the read lock
					upgradeReadLocktoWriteLock(q.transactionID, q.variableName)
					transactionCommits[q.transactionID][q.variableName] = q.variableValue
					q._type = "finished"
					sendAnRPCWriteReply(q.FDValue, "1")
				} 
				
				// if (checkRead == false || (len(i) > 1)){
				// 	//Cant obtain a write lock
				// 	newWriteQ := queueStruct{"write", currentFD, id, val, value}
				// 	queue = append(queue, newWriteQ)
				// 	return &MyRpcWriteIReply{nil}
				// } 
			} else {
				continue
			}
		}

    }
}


func recoverFromLog(){

	tmap := make(map[string]string)

	//Recovering from the store

	FileNames, status := altEthos.SubFiles(path + "store/")
	if status != syscall.StatusOk {
		log.Fatalf("Error fetching files in %v\n", path)
	}
	
	for i := 0; i < len(FileNames); i++ {
		log.Printf(path, FileNames[i])
		var newString kernelTypes.String
		status = altEthos.Read(path + "store/" + FileNames[i], &newString)
		if status != syscall.StatusOk {
			log.Fatalf("Error reading box file at %v/%v\n", path, FileNames[i])
		}

		tmap[FileNames[i]] = string(newString)
	}

	//Recovering from the log

	FileNames, status = altEthos.SubFiles(path + "log/")
	if status != syscall.StatusOk {
		log.Fatalf("Error fetching files in %v\n", path)
	}

	for i := 0; i < len(FileNames); i++ {
		log.Printf(path, FileNames[i])
		var newLog Databaselog
		status = altEthos.Read(path + "log/" + FileNames[i], &newLog)
		if status != syscall.StatusOk {
			log.Fatalf("Error reading box file at %v/%v\n", path, FileNames[i])
		}
		tmap[newLog.name] = string(newLog.value)
	}

	//deleting all the files
	for i := 0; i < len(FileNames); i++ {
		altEthos.FileRemove(path + "log/" + FileNames[i])
		if status != syscall.StatusOk {
			log.Fatalf("Error deleting files in %v\n", path)
		}
	}

	//writing to the store
	for k, v := range tmap { 
		writeToFile(k,string(v))
		log.Printf("Writing to file\n")
	}

}


func main () {

	altEthos.LogToDirectory("test/myRpcServer")
	log.Printf("Database Server: Initializing...\n")

	listeningFd, status := altEthos.Advertise("myRpc")
	if status != syscall.StatusOk {
		log.Printf("Advertising service failed: %s\n", status)
		altEthos.Exit(status)
	}
	log.Printf("Database Server: Done advertising...\n")

	checkDirectory := altEthos.IsDirectory(path)
	if checkDirectory == false {
		log.Println("Directory does not exist ", path, checkDirectory)
		log.Println("Creating Directory")


		log.Printf("Database Server: Creating server directory...\n")

		status = altEthos.DirectoryCreate(path, &pathTypeServer, "all")
		if status != syscall.StatusOk {
			log.Println("Directory Create Failed ", path, status)
			altEthos.Exit(status)
		}

		log.Printf("Database Server: Creating log directory...\n")

		status = altEthos.DirectoryCreate(path + "log/" ,  &logType, "all")
		if status != syscall.StatusOk {
			log.Println("Directory Create Failed ", path, status)
			altEthos.Exit(status)
		}

		log.Printf("Database Server: Creating store directory...\n")

		status = altEthos.DirectoryCreate(path + "store/" ,  &storeType, "all")
		if status != syscall.StatusOk {
			log.Println("Directory Create Failed ", path, status)
			altEthos.Exit(status)
		}
	} else {
		recoverFromLog()
	}

	var tree altEthos.EventTreeSlice
	var next []syscall.EventId

	t := MyRpc{}
	event, status := altEthos.ImportAsync(listeningFd, &t, CustomHandleImport)
	if status != syscall.StatusOk {
		log.Println("Import failed")
		return
	}
	next = append(next, event)
	tree = altEthos.WaitTreeCreateOr(next)

	for {
		tree, _ = altEthos.Block(tree)
		completed, pending :=  altEthos.GetTreeEvents(tree)
		for _, eventId := range completed {
			eventInfo, status := altEthos.OnComplete(eventId)
			if status != syscall.StatusOk {
				log.Println("OnComplete failed", eventInfo, status)
				return
			}
			currentEventID = eventId
			eventInfo.Do()
		}
		next = nil
		next = append(next, pending...)
		next = append(next, altEthos.RetrievePostedEvents()...)
		tree = altEthos.WaitTreeCreateOr(next)
	}
}

// HandleImport handles multiple connections concurrently
//
// When an event occurs:
//  1. start a handle on that event
//  2. import the next connection
func CustomHandleImport(eventInfo altEthos.ImportEventInfo) {
	// start up the read on the imported netFd
	event, status := altEthos.ReadRpcStreamAsync(eventInfo.ReturnedFd, eventInfo.I, altEthos.HandleRpc)
	if status != syscall.StatusOk {
		log.Println("Read Failed")
		return
	}

	event_fd[event] = eventInfo.ReturnedFd
	altEthos.PostEvent(event)

	// start up a new import
	event, status = altEthos.ImportAsync(eventInfo.Fd, eventInfo.I, CustomHandleImport)
	if status != syscall.StatusOk {
		log.Println("Import failed")
		return
	}

	altEthos.PostEvent(event)
}