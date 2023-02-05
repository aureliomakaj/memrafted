curl -H "Content-type: application/json" -d '{ "name": "server1" }' 'http://127.0.0.1:8081/add-cache'

make_json () {
    json="{ \"key\": \"$1\", \"value\": \"$2\", \"exp_time\": $3 }"
}

make_json "k_1" "v_1" 300
curl -XPOST -H "Content-type: application/json" -d "$json" 'http://127.0.0.1:8081/set-key'

make_json "k_2" "v_2" 300
curl -XPOST -H "Content-type: application/json" -d "$json" 'http://127.0.0.1:8081/set-key'

make_json "k_3" "v_3" 300
curl -XPOST -H "Content-type: application/json" -d "$json" 'http://127.0.0.1:8081/set-key'
