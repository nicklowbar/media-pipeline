docker build -t media-pipeline:test .
TMP=$(mktemp -t docker-save-XXXXXX.tar); sudo docker save -o "$TMP" media-pipeline:test && sudo chown $USER "$TMP" && docker --context $1 load -i "$TMP" && rm -f "$TMP"
