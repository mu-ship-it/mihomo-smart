#!/bin/sh
# Run on the Mini: copies binaries + deploy files to the Surface, pipes the subscription link
# from 1Password straight into a root-only file (never printed), and runs setup.sh.
# usage: push.sh <dir with mihomo-smart, mihomo-stock, soak>
set -eu
BIN=$1
HOST=root@100.79.49.101
HERE=$(cd "$(dirname "$0")" && pwd)
ssh -o BatchMode=yes $HOST 'umask 077; mkdir -p /opt/soak/bin /opt/soak/deploy /opt/soak/secrets && chmod 700 /opt/soak /opt/soak/secrets'
for b in mihomo-smart mihomo-stock soak; do
  gzip -9c "$BIN/$b" | ssh -o BatchMode=yes $HOST "gunzip > /opt/soak/bin/$b && chmod 755 /opt/soak/bin/$b"
done
scp -q "$HERE/unit.template" "$HERE/soak-loadgen.service" "$HERE/config.template.yaml" "$HERE/loadgen.template.json" $HOST:/opt/soak/deploy/
scp -q "$HERE/setup.sh" "$HERE/stop.sh" "$HERE/remove.sh" $HOST:/opt/soak/
OP_SERVICE_ACCOUNT_TOKEN=$(cat ~/tasks-bot/.op_service_account_token)
env -i HOME="$HOME" PATH="$PATH" OP_SERVICE_ACCOUNT_TOKEN="$OP_SERVICE_ACCOUNT_TOKEN" \
  op item get w4o4pn4otrjqt5fujgzhmyubbi --vault PipelineLift --fields credential --reveal |
  ssh -o BatchMode=yes $HOST 'umask 077; tr -d "\r\n" > /opt/soak/secrets/sub_url; chmod 600 /opt/soak/secrets/sub_url'
ssh -o BatchMode=yes $HOST 'sh /opt/soak/setup.sh'
