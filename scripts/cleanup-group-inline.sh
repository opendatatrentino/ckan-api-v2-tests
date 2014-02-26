#!/bin/bash

# Perform "inline" cleanup of a group, using a temporary file

TEMPFILE="$( tempfile )"

if [ -z "$1" ]; then
    echo "Usage: $0 <files..>"
    exit 1
fi

JQ_QUERY="{description,extras,id,image_url,name,tags,title}"

for INPUT in "$@"; do
    cat "$INPUT" | jq "$JQ_QUERY" > "$TEMPFILE"
    cat "$TEMPFILE" > "$INPUT"
done

rm "$TEMPFILE"