# dvbraptor SBC provisioning

Ansible playbook to provision a Debian Trixie SBC as a DVB-S live-video
transmitter: installs deps, the Rust toolchain, builds `raptorq`, deploys the
GNU Radio flowgraph + encoder script, and wires up the systemd boot sequence.

## Prerequisites (control machine)

`ansible` (core). Provided by the repo nix devShell — just `nix develop`.
Uses only builtin modules; no galaxy collections required.

## Usage

1. `nix develop` (gets ansible + ansible-lint).
2. Edit `inventory.ini` (host/IP/ssh user).
3. Adjust `group_vars/all.yml` if needed (`ping_target`, `eth_iface`, `encoder`).
4. Run:

```sh
cd ansible
ansible-playbook playbook.yml
```

`raptorq` is built on the target (`cargo build --release`) on first run and
again whenever the source changes. Re-running the playbook is idempotent.

To start the link immediately after provisioning (default is enable-only, so a
remote run doesn't restart a live transmitter):

```sh
ansible-playbook playbook.yml -e start_now=true
```

## Boot sequence (systemd)

```
live-video-eee.service          disable EEE on eth0 (ethtool)
   -> wait-network@10.60.70.2    ping ground station until reachable (forever)
      -> live-video-tx.service   gnuradio/dvbs_tx.py (PlutoSDR)
         -> live-video-enc.service  run.sh pi264 (gstreamer -> raptorq-enc)
```

Both `tx` and `enc` use `Restart=always`, `RestartSec=2`,
`StartLimitIntervalSec=0` — they retry forever with no backoff or attempt cap
and recover independently via the shared ZMQ socket at
`/run/live-video/raptorq.sock`. `live-video.target` groups everything and is
enabled on `multi-user.target`.

## Layout on target

```
/etc/live-video/
  raptorq/                 synced source (built here)
  gnuradio/                dvbs_tx.py (+ .grc)
  run.sh                   rendered from templates/run.sh.j2
  bin/{raptorq-enc,raptorq-dec}
```
Owned by `livevideo:livevideo`; `livevideo` is in `input` and `video` groups.
A udev rule symlinks the first USB camera to `/dev/rocket-cam`.
