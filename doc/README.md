# Ecallmgr Extensions

Private set of modules that extend ecallmgr to be more efficient at higher volumes of traffic.

## Config

See [./config.md](config) for `ecallmgr` system configs.

## Metaflows

DTMF-intiated metaflows are only triggered on bridged channels. Single-leg channels will not have a DTMF listener started.

API calls to the `/channels` endpoint will be handled however ('action' and 'flow' payloads).
