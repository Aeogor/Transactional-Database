Name  : Srinivas C Lingutla
UIN   : 655115444
NetID : slingu2

Platform USED: ETHOS

------------------------------ Homework 4 - CS 485 -----------------------------------

 === BUILD AND RUN ====
 make install
 cd server
 sudo -E ethosRun

This currently cleans the folder and builds all the files
This will start the ethos instance

In an another terminal, navigate to server folder

 etAl server.ethos

This will give you the terminal access for the ethos instance
Navigate to /programs folder and run myRpcClient

 cd /programs
 myRpcClient

Similarly you can connect another user using 
 
 et server.ethos

--------------- IMPLEMENTATION ---------------

Currently, the server starts when the ethos instance gets launched. It 
advertises itself and waits for connections. It uses importAsync to accept
several connections and add it to a event tree. 

The client should start a transaction before attempting to query the 
database. The start transaction returns a transaction ID which will be 
used through the transaction queries like read, write, end, and abort. 
The read will prompt the user to enter a variable name to read. And 
similarly the write will prompt the user to enter a variable name and 
value that he wants to write. If there is a variable already present, then
the value will be overwritten. End will commit all the remaining changes
to the database and return. Abort will remove any changes and return. 

The server will keep track of the variable in memory until committing. 
The read locks and the write locks are stored in maps and the queue is 
stored as a list. The queue contains any pending reads and writes that are
not completed due to locks present on those variables. The queue gets 
processed after End and Abort calls. 

Commands
---------------------
-start	: Start a transaction
-end	: End a transaction
-read	: Read a varible from the database
-write	: Write to a variable in the database
-abort	: Abort a transaction
---------------------

I wasn't completely able to test the recovery for the database, as i had 
trouble getting the server to restart once ended. But the code is written 
and should work as expected .

-------------------------------------------------------
FILES INCLUDED

|-- Makefile
|-- README.txt
|-- myRpc.t
|-- myRpcClient.go
|-- myRpcService.go

0 directories, 5 files

-------------------------------