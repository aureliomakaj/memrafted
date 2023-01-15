Simple implementation of a caching systems using HTTP requests.

Api calls:
GET /get-key?key=key-name
Function: Get the value cached at a given key

GET /print-internally
Function: Dump the content of the servers. Dumped as a log info

POST /set-key 
with the following JSON body:
{ 
    "key": "key_1", 
    "value": { 
        "variant": "String", 
        "content": "value_1"  
    }, 
    "expiration": 60 
}
Function: Cache a value at a given key

POST /add-server
with the following JSON body:
{ 
    "name": "server1" 
}
Function: Add a server. This should simulate the scalability option
