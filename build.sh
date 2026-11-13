#!/bin/bash

function fail() {
    local error="${*:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

joined_arguments=""

function build_and_run() {
    local connector="$1"
    if [[ $2 == "driver" ]]; then
        path=drivers/$connector
    else
        fail "The argument does not have a recognized prefix."
    fi
    

    [[ -n "$OLAKE_SKIP_MOD_TIDY" ]] || (cd $path && go mod tidy)

    # prepare.<driver> provisions build deps (db2: the IBM clidriver) and GO_ENV.<driver>
    # supplies its cgo env, so this covers the whole driver-specific setup.
    make dev.$connector.build || fail "build failed"

    cd $path || fail "Failed to navigate to path: $path"

    echo "============================== Executing connector: $connector with args [$joined_arguments] =============================="
    ./olake $joined_arguments
}

if [ $# -gt 0 ]; then
    argument="$1"

    # Capture and join remaining arguments, skipping the first one
    remaining_arguments=("${@:2}")
    joined_arguments=$(
        IFS=' '
        echo "${remaining_arguments[*]}"
    )

    if [[ $argument == driver-* ]]; then
        driver="${argument#driver-}"
        echo "============================== Building driver: $driver =============================="
        build_and_run "$driver" "driver" "$joined_arguments"
    else
        fail "The argument does not have a recognized prefix."
    fi
else
    fail "No arguments provided."
fi