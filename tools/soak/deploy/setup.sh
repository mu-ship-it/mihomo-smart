#!/bin/sh
# Generates the instance configs from the root-only subscription link and installs the three
# soak units. Files live only under /opt/soak; systemd gets symlinks to the unit files.
set -eu
umask 077
cd /opt/soak
test -s secrets/sub_url
# fetch the subscription once (curl does not print the URL on errors); both instances get
# byte-identical copies, so they see exactly the same nodes
curl -sS -A clash.meta --retry 3 --max-time 60 -o secrets/sub.yaml "$(cat secrets/sub_url)"
chmod 600 secrets/sub.yaml
test -s secrets/sub.yaml
python3 -I - <<'PY'
import secrets, json, os
tpl = open("/opt/soak/deploy/config.template.yaml", encoding="utf-8").read()
insts = [("smart", 27890, 29090, "smart"), ("stock", 27891, 29091, "url-test")]
lg = json.load(open("/opt/soak/deploy/loadgen.template.json"))
lg["instances"] = []
for name, mixed, ctl, gtype in insts:
    os.makedirs(f"/opt/soak/{name}", mode=0o700, exist_ok=True)
    sec = secrets.token_hex(16)
    cfg = (tpl.replace("@NAME@", name).replace("@MIXED@", str(mixed)).replace("@CTL@", str(ctl))
              .replace("@SECRET@", sec).replace("@GROUPTYPE@", gtype))
    path = f"/opt/soak/{name}/config.yaml"
    with open(path, "w", encoding="utf-8") as f:
        f.write(cfg)
    os.chmod(path, 0o600)
    os.makedirs(f"/opt/soak/{name}/providers", mode=0o700, exist_ok=True)
    prov = f"/opt/soak/{name}/providers/kycloud.yaml"
    with open("/opt/soak/secrets/sub.yaml", "rb") as src, open(prov, "wb") as dst:
        dst.write(src.read())
    os.chmod(prov, 0o600)
    lg["instances"].append({"name": name, "proxy": f"127.0.0.1:{mixed}", "controller": f"http://127.0.0.1:{ctl}",
                            "secret": sec, "group": "Soak", "unit": f"{name}-soak.service"})
with open("/opt/soak/loadgen.json", "w") as f:
    json.dump(lg, f, indent=2)
os.chmod("/opt/soak/loadgen.json", 0o600)
PY
mkdir -p /opt/soak/units /opt/soak/data
for n in smart stock; do
  sed "s/@NAME@/$n/g" deploy/unit.template > units/$n-soak.service
done
cp deploy/soak-loadgen.service units/
chmod 644 units/*.service
chmod 755 bin/* remove.sh stop.sh
for u in smart-soak stock-soak soak-loadgen; do
  systemctl link /opt/soak/units/$u.service >/dev/null
done
systemctl daemon-reload
echo "setup ok"
