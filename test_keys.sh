curl -H "Content-type: application/json" -d '{ "name": "server1" }' 'http://127.0.0.1:8081/add-cache'

curl -XPOST -H "Content-type: application/json" -d '{ "key": "s_1", "value": "v_1", "expiration": 60 }' 'http://127.0.0.1:8081/set-key'

curl -XPOST -H "Content-type: application/json" -d '{ "key": "s_2", "value": "v_2", "expiration": 60 }' 'http://127.0.0.1:8081/set-key'

curl -XPOST -H "Content-type: application/json" -d '{ "key": "s_3", "value": "v_3", "expiration": 60 }' 'http://127.0.0.1:8081/set-key'




