# FixBadMod

FixBadMod scans world chunks and removes/replaces broken block or item references from removed mods.

## Commands

```txt
/fixbadmod scan --world=<world>
/fixbadmod execute --world=<world>
/fixbadmod status --world=<world>
/fixbadmod cancel --world=<world>
/fixbadmod reload
```

## Config

Main config file:

`mods/FixBadMod/config.json`

Important scan options:

- `maxReplacementsPerChunk`: `-1` for unlimited in a chunk.
- `matchBreakdownLimit`: max entries printed in SCAN/APPLY breakdown logs.
- `autoDeleteUnknownKeys`: when `true`, unknown keys are auto-removed if no explicit rule matches.

## Templates

Template folder:

`mods/FixBadMod/templates/`

The plugin auto-loads all `*.json` templates in this folder on startup and on `/fixbadmod reload`.

Built-in template shipped with the plugin:

`mods/FixBadMod/templates/nocube_tavern.json`

Template format:

```json
{
  "enable": false,
  "description": "Your template description",
  "rules": [
    {
      "enabled": true,
      "match": "NoCube_Tavern_",
      "mode": "prefix",
      "replaceWith": "__DELETE__"
    }
  ]
}
```

Only templates with `enable: true` are applied.

## Build

```powershell
.\gradlew.bat clean jar
```

Output files in `build/libs/`:

- `FixBadMod_1.0.1.jar`
- `FixBadMod_1,0.1.jar`
