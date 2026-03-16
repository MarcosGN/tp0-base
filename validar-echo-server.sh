#!/bin/bash
MESSAGE="Hello, Echo Server!"
RESPONSE=$(docker run --rm --network="tp0_testing_net" alpine /bin/sh -c "echo '$MESSAGE' | nc "server" 12345")

if [ "$RESPONSE" = "$MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi