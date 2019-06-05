MyRpc interface {
	TransactionStartI() (id int64, status string)
	TransactionEndI(id int64) (status string) 
	ReadI(id int64, variableName string) (value string, status string)
	WriteI(id int64,  val string, value string) (status string) 
	AbortI(id int64) (status string) 
}

Databaselog struct {
	name string
	value string
}

