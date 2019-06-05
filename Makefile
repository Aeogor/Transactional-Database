export UDIR= .
export GOC = x86_64-xen-ethos-6g
export GOL = x86_64-xen-ethos-6l
export ETN2GO = etn2go
export ET2G   = et2g
export EG2GO  = eg2go

export GOARCH = amd64
export TARGET_ARCH = x86_64
export GOETHOSINCLUDE=/usr/lib64/go/pkg/ethos_$(GOARCH)
export GOLINUXINCLUDE=/usr/lib64/go/pkg/linux_$(GOARCH)


export ETHOSROOT=server/rootfs
export MINIMALTDROOT=server/minimaltdfs


.PHONY: all install clean
all:  myRpcClient myRpcService

myRpc.go: myRpc.t
	$(ETN2GO) . myRpc main $^

myRpcService: myRpcService.go myRpc.go
	ethosGo $^ 

myRpcClient: myRpcClient.go myRpc.go
	ethosGo $^ 

# install types, service,
install: clean myRpcClient myRpcService
	(ethosParams server && cd server && ethosMinimaltdBuilder)
	ethosTypeInstall myRpc
	ethosDirCreate $(ETHOSROOT)/services/myRpc   $(ETHOSROOT)/types/spec/myRpc/MyRpc all
	install -D  myRpcService myRpcClient         $(ETHOSROOT)/programs
	ethosStringEncode /programs/myRpcService    > $(ETHOSROOT)/etc/init/services/myRpcService
	

# remove build artifacts
clean:
	sudo rm -rf server
	rm -rf myRpc/ myRpcIndex/
	rm -f myRpc.go
	rm -f myRpcService
	rm -f myRpcService.goo.ethos
	rm -f myRpcClient
	rm -f myRpcClient.goo.ethos

