#goDB

###Features

1. Replicated
2. Consistent
3. Fault Tolerant

========

###Supported Operations

1. Get
2. Put
3. Delete

========

###Mechnisms 

#####Raft Leader Election for Consensus

Makes use of Raft Leader election protocol to achieve consensus. The system ensures that there is only one leader at a given point of time. It is fault tolerant. In the event of shutdown of the leader, a new leader is immediately elected. The system has been tested rigorously to maintain this property.

#####Raft Log Replication for Consistency

Consistency is achieved by using log replication. Every operation is declared to be committed only if it is committed in the log of majority of the servers. It it fails to do so then the operation is cancelled.

It makes use of leveldb to store the log entries

###Instructions for installing

#####Set GOPATH

``> export GOPATH=<directory>``

``> go get github.com/nikolodien/goDB``

#####The next two steps can be skipped if you don't get any messages

``> go build github.com/nikolodien/goDB``

``> go install $GOPATH/src/github.com/nikolodien/goDB/broker.go``

========

###Usage

A new instance of the server has to be created using the <code>statemachine.New()</code> function

After that, the client needs to connect to the server as follows

<code>client, err := rpc.DialHTTP("tcp", host:port)</code>

Each operation is an rpc call to the server

1. Get  
    <code>var val statemachine.Val</code>  
		<code>var key = "key_string"</code>  
		<code>err = client.Call("KVStore.Get", key, &val)</code>  
2. Put   
    <code>keyValue := statemachine.KeyValue{key, i}</code>  
		<code>var reply int</code>  
		<code>err = client.Call("KVStore.Put", keyValue, &reply)</code>  
3. Delete  
    <code>var reply int</code>  
    <code>key := "key_string"</code>  
		<code>err = client.Call("KVStore.Del", key, &reply)</code>  

========
