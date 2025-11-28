#!/bin/bash

# go run main.go --serverID=1 & go run main.go --serverID=2 & go run main.go --serverID=3

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Running script from: $SCRIPT_DIR"

# Move to that directory (optional, if you want commands to run from here)
cd "$SCRIPT_DIR" || exit
# List of commands you want to run in separate terminals
commands=(
    "go run main.go --serverID=1"
    "go run main.go --serverID=2"
    "go run main.go --serverID=3"
    "go run main.go --serverID=4"
    "go run main.go --serverID=5"
    "go run main.go --serverID=6"
    "go run main.go --serverID=7"
    "go run main.go --serverID=8"
    "go run main.go --serverID=9"
)

# Loop through commands and open each in a new terminal
for cmd in "${commands[@]}"
do
    echo "Opening terminal for: $cmd"
    osascript -e "tell application \"Terminal\" to do script \"cd $SCRIPT_DIR; $cmd\""
done

sleep 3
osascript -e "tell application \"Terminal\" to do script \"cd $SCRIPT_DIR; go run ./client\""

