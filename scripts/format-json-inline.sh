#!/bin/bash

# Pretty-format a json file, in place

TEMPFILE="$( tempfile )"

if [ -z "$1" ]; then
    echo "Usage: $0 <files..>"
    exit 1
fi

for INPUT in "$@"; do
    cat "$INPUT" | jq . > "$TEMPFILE"
    cat "$TEMPFILE" > "$INPUT"
done

rm "$TEMPFILE"
