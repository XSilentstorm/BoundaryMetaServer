// =============================================================================
//  Boundary MetaServer — reverse-engineered game backend
// =============================================================================

// -----------------------------------------------------------------------------
//  Dependencies
// -----------------------------------------------------------------------------

const express = require("express");
const bodyParser = require("body-parser");
const net = require("net");
const protobuf = require("protobufjs");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const udp = require("dgram");
const { serialize } = require("v8"); // retained (present in original)

// -----------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------

const HTTP_PORT = Number(process.env.PORT || 8000);
const RPC_PORT = Number(process.env.RPC_PORT || 6969);
const MATCHMAKING_PORT = Number(process.env.MATCHMAKING_PORT || 9000);
const MATCHMAKING_HOST = process.env.MATCHMAKING_HOST || "132.226.204.81";

// Gate/handshake token echoed by the client after /connectServer
const TEST_GATE_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30";

const RPC_ENDPOINT = `${MATCHMAKING_HOST}:${RPC_PORT}`;

// UDP QoS probe bytes (matchmaking)
const QOS_REQUEST_BYTE = 0x59;
const QOS_RESPONSE_BYTE = 0x95;

// Length-prefixed TCP framing: 4-byte BE length + payload
const TCP_LENGTH_HEADER_SIZE = 4;
const MAX_TCP_FRAME_PAYLOAD = 16 * 1024 * 1024;

// Raw echo frame seen during early connection (length=2, payload="//")
const TCP_HANDSHAKE_ECHO_HEX = "000000022f2f";

const DATA_DIR = path.join(__dirname, "data");
const LOG_DIR = path.join(__dirname, "logs");

// ---- Per-player archive paths ----

/** Map gateToken → playerId (populated by /connectServer, consumed by TCP handshake). */
const gateTokenToPlayerId = new Map();

function getArchivePath(playerId) {
  return path.join(DATA_DIR, `${playerId}.json`);
}

function buildDefaults(specs) {
  const roles = {};
  for (const [roleId, s] of Object.entries(specs)) {
    const up = roleId.toUpperCase();
    roles[roleId] = {
      RoleID: roleId,
      PrimaryWeapon: s.PW,
      SecondWeapon: s.SW,
      LeftPylon: s.LP,
      MeleeWeapon: "MELEE-KNIFE",
      MobilityModule: up + "_FCM-GRAPPLE",
      _SkinBase: up + "_ORIGINAL",
      _SkinPaint: up + "_ORIGINAL_PTOriginal",
      _Cosmetic9: s.C9,
      _Cosmetic10: "HONONE",
    };
  }
  return roles;
}

const DEFAULT_ROLES = buildDefaults({
  PEACE:  { PW:"PEACE_RU-AKM",    SW:"PEACE_RU-APS",   LP:"PEACE_TAC-EMP",       C9:"ABGOrlanDefault" },
  PROBE:  { PW:"PROBE_GSW-DMR",   SW:"PROBE_GSW-FOP",  LP:"PROBE_MISSILE-HIVE",   C9:"ABGProbeDefault" },
  Sniper: { PW:"SNIPER_GSW-PSR",  SW:"SNIPER_GSW-CDP", LP:"SNIPER_INFO-SNAPSHOT", C9:"ABGYangDefault"   },
  FORT:   { PW:"FORT_GSW-MG",     SW:"FORT_GSW-IDW",   LP:"FORT_TAC-ADS",         C9:"ABGFortDefault"   },
  FIXER:  { PW:"FIXER_GSW-PCC",   SW:"FIXER_GSW-FOP",  LP:"FIXER_TAC-MED",         C9:"ABGDocDefault"   },
  SPIKE:  { PW:"SPIKE_GSW-SG",    SW:"SPIKE_GSW-IDW",  LP:"SPIKE_SMK-SQUID",       C9:"ABGSpikeDefault"  },
});

function loadArchiveFor(playerId) {
  const p = getArchivePath(playerId);
  try {
    if (!fs.existsSync(p)) {
      const archive = JSON.parse(JSON.stringify(DEFAULT_ROLES));
      saveArchiveFor(playerId, archive);
      logger.info(`[PERSIST] Initialized 6-role defaults for new player ${playerId}`);
      return archive;
    }
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    const roles = raw && raw.roles && typeof raw.roles === "object" ? raw.roles : {};
    logger.info(`[PERSIST] Loaded ${Object.keys(roles).length} roles for ${playerId}`);
    return roles;
  } catch (e) {
    logger.error(`[PERSIST] Load failed for ${playerId}:`, e.message);
    return {};
  }
}

function saveArchiveFor(playerId, archive) {
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(
      getArchivePath(playerId),
      JSON.stringify({ savedAt: new Date().toISOString(), roles: archive }, null, 2),
      "utf8"
    );
  } catch (e) {
    logger.error(`[PERSIST] Save failed for ${playerId}:`, e.message);
  }
}

const PROTO_REQUEST_WRAPPER = "./game/proto/Request/RequestWrapper.proto";
const PROTO_RESPONSE_WRAPPER = "./game/proto/Response/ResponseWrapper.proto";
const PROTO_JSON_RESPONSE_WRAPPER = "./game/proto/Response/JSONResponseWrapper.proto";

const TEMP_USER_ID = "76561198211631084";

// UpdateRoleArchiveV2 slot enum → PlayerRoleData field name
const SLOT_TO_FIELD = {
  1: "PrimaryWeapon",
  2: "SecondWeapon",
  3: "MeleeWeapon",
  4: "MobilityModule",
  5: "LeftPylon",
  6: "RightPylon",
};

// Internal store keys for non-weapon data
const SKIN_BASE_KEY = "_SkinBase";
const SKIN_PAINT_KEY = "_SkinPaint";
const COSMETIC_KEY_PREFIX = "_Cosmetic";
const WEAPON_CONFIG_KEY = "_WeaponConfigs";

const PATCHNOTES_4012026_TEXT =
  "Welcome to the second round of patchnotes! Most of this is bugfix-focused, and those fixes might not even work yet! Fun!\n\nNew Features:\n- PvE Match Support! This should (theoretically) allow you to take on Hard bots, either solo or CoOp! This still has to be hosted, but we might run some at some point!\n- Randomized map selection, from all available Boundary maps!\n- Proper TDM mode setup, first to 75 kills wins, should last 10 min!\n\nBugfixes:\n- HOPEFULLY fixed the 999/999 spawn bug, though we're gonna have to confirm this in a second to see if I actually fixed it or not!\n- Fixed up the logic server a little bit to make it somewhat more reliable, shouldn't crash as often now\n\nThat's it for today, hope y'all enjoy!";

const PATCHNOTES_3312026_TEXT =
  "Welcome to the first round of patches for Project Rebound!\nNew Features:\n- Basic emulation of the Logic Server. This allows you to see the news (hi), adjust settings, and not have to reboot the game for every match\n- In-Game Medals & Scoring! Go for those headshots :)\nBugfixes:\n- Fixed several bugs causing respawning early to softlock the game. There is still one more bug I'm working out here, but there should already be improvement here.\n- Upgraded to 128 tick servers! This might get reverted if horrific things happen, but for now enjoy 128tick Boundary!\n- Various optimizations to backend tech, should make your matches significantly more stable!";

const ALPHA_TEXT =
  "Welcome to the Project Rebound Alpha. Please be patient and respectful to me & your fellow playtesters. Matchmaking will prioritize short queues over full matches, so feel free to coordinate in the discord to get games going.";

const PLAYLISTS_JSON = {
  PVP: [
    {
      Name: "Playtest",
      Title: [{ en: "Playtest" }],
      Description: [, { en: "Playtest a very early version of Project Rebound" }],
      SecondaryDescription: [
        { en: "Please report any bugs to @systemdev in the Boundary discord" },
      ],
      BigTitle: [{ en: "Playtest" }],
      BigDescription: [{ en: "Playtest a very early version of Project Rebound" }],
      PlotImage: [{ zh: "Capture" }, { en: "Capture" }],
      LargePlotImage: [{ zh: "Capture" }, { en: "Capture" }],
      GameModeList: ["Purge"],
      bHasFilter: false,
      bIsLive: true,
      Priority: 1,
      StartTime: 0,
      StopTime: 0,
    },
  ],
};

const matchmakingUDPServerDiscoveryPayload = {
  servers: [
    {
      location_id: 6,
      region_id: "336d1f3e-3ecb-11eb-a7dc-3b7705f20f56",
      ipv4: MATCHMAKING_HOST,
      ipv6: "",
      port: MATCHMAKING_PORT,
    },
    {
      location_id: 10,
      region_id: "11111111-2222-4333-8444-555555555501",
      ipv4: MATCHMAKING_HOST,
      ipv6: "",
      port: MATCHMAKING_PORT,
    },
  ],
};

// protobufjs toObject options — keep identical to original
const objectOptions = {
  Enums: String, // enums as string names
  longs: String, // longs as strings (requires long.js)
  defaults: true, // includes default values
  arrays: true, // populates empty arrays (repeated fields) even if defaults=false
  objects: true, // populates empty objects (map fields) even if defaults=false
  oneofs: true, // includes virtual oneof fields set to the present field's name);
};

let partyPresence = "InMatching";

/** Per-socket role archive cache. Pinned by gateToken echo handshake. */
// (was: let roleArchive = {}; — now lives on socket._roleArchive)

// -----------------------------------------------------------------------------
//  Logger
// -----------------------------------------------------------------------------

const LOG_LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const LOG_LEVEL_VALUE = LOG_LEVELS[LOG_LEVEL] !== undefined ? LOG_LEVELS[LOG_LEVEL] : LOG_LEVELS.info;

function logTimestamp() {
  return new Date().toISOString();
}

const logger = {
  debug: (...args) => { if (LOG_LEVEL_VALUE <= LOG_LEVELS.debug) console.log(`[${logTimestamp()}] [DEBUG]`, ...args); },
  info:  (...args) => { if (LOG_LEVEL_VALUE <= LOG_LEVELS.info)  console.log(`[${logTimestamp()}] [INFO]`, ...args); },
  warn:  (...args) => { if (LOG_LEVEL_VALUE <= LOG_LEVELS.warn)  console.warn(`[${logTimestamp()}] [WARN]`, ...args); },
  error: (...args) => { if (LOG_LEVEL_VALUE <= LOG_LEVELS.error) console.error(`[${logTimestamp()}] [ERROR]`, ...args); },
};

// -----------------------------------------------------------------------------
//  Protobuf type cache + response helpers
// -----------------------------------------------------------------------------

const protoTypeCache = new Map();

function loadProtoType(protoPath, typeName) {
  const key = `${protoPath}\0${typeName}`;
  let type = protoTypeCache.get(key);
  if (!type) {
    const root = protobuf.loadSync(protoPath);
    type = root.lookupType(typeName);
    protoTypeCache.set(key, type);
  }
  return type;
}

function wrapResponse(messageId, rpcPath, responseBytes) {
  const responseWrapperType = loadProtoType(
    PROTO_RESPONSE_WRAPPER,
    "ProjectBoundary.ResponseWrapper"
  );
  const responseWrapper = responseWrapperType.create({
    MessageId: messageId,
    RPCPath: rpcPath,
    ErrorCode: 0,
    Message: responseBytes,
  });
  const responsePayload = responseWrapperType.encode(responseWrapper).finish();
  const responseLengthHeader = Buffer.alloc(TCP_LENGTH_HEADER_SIZE);
  responseLengthHeader.writeUint32BE(responsePayload.length);
  return Buffer.concat([responseLengthHeader, responsePayload]);
}

function wrapJsonResponse(messageId, rpcPath, responseJson) {
  const responseWrapperType = loadProtoType(
    PROTO_JSON_RESPONSE_WRAPPER,
    "ProjectBoundary.JSONResponseWrapper"
  );
  const responseWrapper = responseWrapperType.create({
    MessageId: messageId,
    RPCPath: rpcPath,
    ErrorCode: 0,
    JSONMessage: JSON.stringify(responseJson),
  });
  const responsePayload = responseWrapperType.encode(responseWrapper).finish();
  const responseLengthHeader = Buffer.alloc(TCP_LENGTH_HEADER_SIZE);
  responseLengthHeader.writeUint32BE(responsePayload.length);
  return Buffer.concat([responseLengthHeader, responsePayload]);
}

function encodeProtoMessage(protoPath, typeName, payload) {
  const type = loadProtoType(protoPath, typeName);
  return type.encode(type.create(payload)).finish();
}

function sendProtoResponse(socket, messageId, rpcPath, protoPath, typeName, payload) {
  const responseBytes = encodeProtoMessage(protoPath, typeName, payload);
  socket.write(wrapResponse(messageId, rpcPath, responseBytes));
  return responseBytes;
}

function sendEncodedProtoResponse(socket, messageId, rpcPath, responseBytes) {
  socket.write(wrapResponse(messageId, rpcPath, responseBytes));
}

function sendJsonResponse(socket, messageId, rpcPath, responseJson) {
  socket.write(wrapJsonResponse(messageId, rpcPath, responseJson));
}

/** Encode a response with a single StatusCode: 0 varint field */
function encodeStatusCodeResponse(protoPath, messageName) {
  return encodeProtoMessage(protoPath, messageName, { StatusCode: 0 });
}

function decodeRequestMessage(protoPath, typeName, messageBytes) {
  const type = loadProtoType(protoPath, typeName);
  return type.toObject(type.decode(messageBytes), objectOptions);
}

// -----------------------------------------------------------------------------
//  Notification / region helpers
// -----------------------------------------------------------------------------

function buildNotification(title, content, background, languageCode, platform, timezone) {
  return {
    Id: crypto.randomUUID().toString(),
    Title: title,
    Content: content,
    Background: background,
    LanguageCode: languageCode,
    Platform: platform,
    Unknown1: 1,
    Timezone: timezone,
    Unknown2: 1,
  };
}

function buildRegionList() {
  return [
    {
      RegionId: matchmakingUDPServerDiscoveryPayload.servers[0].region_id,
      RegionName: "us-east1",
    },
    {
      RegionId: matchmakingUDPServerDiscoveryPayload.servers[1].region_id,
      RegionName: "asia-east2",
    },
  ];
}

// =============================================================================
//  Loadout Persistence — role-archive.json store + GetPlayerArchiveV2 encoding
// =============================================================================

// ---- protobuf helpers ----

function encodeVarint(value) {
  if (value < 0) value = 0;
  const parts = [];
  while (value > 0x7f) {
    parts.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  parts.push(value & 0x7f);
  return Buffer.from(parts);
}

function readVarint(buf, offset) {
  let value = 0,
    shift = 0;
  while (offset < buf.length) {
    const b = buf[offset++];
    value |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
    if (shift > 70) break;
  }
  return { value, next: offset };
}

function readLengthDelimited(buf, offset) {
  const { value: len, next } = readVarint(buf, offset);
  if (next + len > buf.length) return { value: Buffer.alloc(0), next: buf.length };
  return { value: buf.subarray(next, next + len), next: next + len };
}

function toBuffer(bytes) {
  if (bytes == null) return Buffer.alloc(0);
  if (Buffer.isBuffer(bytes)) return bytes;
  if (bytes instanceof Uint8Array) return Buffer.from(bytes);
  if (typeof bytes === "string") return Buffer.from(bytes, "binary");
  try {
    return Buffer.from(bytes);
  } catch (_) {
    return Buffer.alloc(0);
  }
}

// ---- Per-player role archive helpers ----

function ensureRoleEntry(archive, roleId) {
  if (!archive[roleId] || typeof archive[roleId] !== "object") {
    archive[roleId] = { RoleID: roleId };
  }
  archive[roleId].RoleID = roleId;
  return archive[roleId];
}

// ---- SkinConfig encoding (field 8) ----
// Codec sub_1409D62E0: field 1 (0x0a) = SuitConfig, field 2 (0x12) = arm badge, field 3 (0x1a) = head ornament.
// SuitConfig sub-message: field 1 (0x0a) = skinItem (model), field 2 (0x12) = paintItem (color).

function encodeSkinConfig(stored) {
  if (!stored[SKIN_BASE_KEY] || stored[SKIN_BASE_KEY] === "None") return null;

  const skinItem = stored[SKIN_BASE_KEY];
  const paintItem =
    stored[SKIN_PAINT_KEY] && stored[SKIN_PAINT_KEY] !== "None"
      ? stored[SKIN_PAINT_KEY]
      : null;

  // SuitConfig: field 1 (0x0a) = skinItem (model), field 2 (0x12) = paintItem (color)
  const suitParts = [];
  const skinBytes = Buffer.from(skinItem, "utf8");
  suitParts.push(Buffer.from([0x0a]), Buffer.from([skinBytes.length]), skinBytes);
  if (paintItem) {
    const paintBytes = Buffer.from(paintItem, "utf8");
    suitParts.push(Buffer.from([0x12]), Buffer.from([paintBytes.length]), paintBytes);
  }
  const suitBody = Buffer.concat(suitParts);

  // SkinConfig: field 1 (0x0a) = SuitConfig, field 2 (0x12) = arm badge, field 3 (0x1a) = head ornament
  const skinParts = [];
  skinParts.push(Buffer.from([0x0a]), encodeVarint(suitBody.length), suitBody);

  // Field 2 (0x12): arm badge (slot 9)
  const armBadge = stored[`${COSMETIC_KEY_PREFIX}9`];
  if (armBadge && armBadge !== "None") {
    const abBytes = Buffer.from(armBadge, "utf8");
    skinParts.push(Buffer.from([0x12]), Buffer.from([abBytes.length]), abBytes);
  }

  // Field 3 (0x1a): head ornament (slot 10)
  const headOrn = stored[`${COSMETIC_KEY_PREFIX}10`];
  if (headOrn && headOrn !== "None") {
    const ornBytes = Buffer.from(headOrn, "utf8");
    skinParts.push(Buffer.from([0x1a]), Buffer.from([ornBytes.length]), ornBytes);
  }

  return Buffer.concat(skinParts);
}

// ---- WeaponConfig encoding (field 9) ----
// Wire format: repeated 0x0a [varint] [sub_1409D2120 blob]
// protobufjs adds the outer 0x4a field wrapper

function encodeWeaponConfig(stored) {
  if (!stored[WEAPON_CONFIG_KEY] || typeof stored[WEAPON_CONFIG_KEY] !== "object") return null;

  const weaponIds = Object.keys(stored[WEAPON_CONFIG_KEY]).filter(
    (k) =>
      typeof stored[WEAPON_CONFIG_KEY][k] === "string" &&
      stored[WEAPON_CONFIG_KEY][k].length > 0
  );
  if (weaponIds.length === 0) return null;

  const parts = [];
  for (const weaponId of weaponIds) {
    const blob = Buffer.from(stored[WEAPON_CONFIG_KEY][weaponId], "base64");
    parts.push(Buffer.from([0x0a]), encodeVarint(blob.length), blob);
  }
  return Buffer.concat(parts);
}

// ---- Decode base64 WeaponArchive protobuf → readable JSON ----
// Wire format (reversed from sub_1409D2120 + sub_1409D26F0):
//   WeaponConfig:
//     field 1 (0x0a): weaponID string
//     field 2 (0x12): repeated WeaponPart sub-messages:
//       field 1 (0x08): slotId varint
//       field 2 (0x12): partId string
//       field 3 (0x1a): PartOrnament { Info: OrnamentInfo { type, id } }
//     field 3 (0x1a): optional WeaponSkin { SkinInfo: OrnamentInfo { type, id } }

// Helper: parse OrnamentInfo { Type, Id } from buffer at offset
function parseOrnamentInfo(buf, offset) {
  let type = null, id = null, pos = offset;
  while (pos < buf.length) {
    let { value: t, next: np } = readVarint(buf, pos); pos = np;
    const fn = t >>> 3;
    if ((t & 0x7) !== 2) break;
    let { value: v, next: nq } = readLengthDelimited(buf, pos); pos = nq;
    if (fn === 1) type = v.toString('utf8');
    else if (fn === 2) id = v.toString('utf8');
  }
  return { type, id, next: pos };
}

// Helper: parse PartOrnament|WeaponSkin wrapper: field 1 (0x0a) contains OrnamentInfo
function parseOrnamentWrapper(buf, offset) {
  let result = null, pos = offset;
  while (pos < buf.length) {
    let { value: t, next: np } = readVarint(buf, pos); pos = np;
    if ((t & 0x7) !== 2) break;
    let { value: inner, next: nq } = readLengthDelimited(buf, pos); pos = nq;
    if ((t >>> 3) === 1) {
      let parsed = parseOrnamentInfo(inner, 0);
      result = { type: parsed.type, id: parsed.id };
    }
  }
  return { ornament: result, next: pos };
}

function decodeWeaponArchive(buf) {
  let i = 0;
  let weaponId = null;
  const parts = [];
  let weaponOrnament = null;

  while (i < buf.length) {
    let { value: tag, next } = readVarint(buf, i);
    i = next;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x7;

    if (wireType !== 2) { i = buf.length; break; }

    let { value, next: n } = readLengthDelimited(buf, i); i = n;

    if (fieldNumber === 1) {
      weaponId = value.toString('utf8');
    } else if (fieldNumber === 2) {
      let j = 0, slotId = null, partId = null, partOrnament = null;
      while (j < value.length) {
        let { value: pt, next: pj } = readVarint(value, j); j = pj;
        const pfn = pt >>> 3, pwt = pt & 0x7;
        if (pwt === 0) {
          let { value: pv, next: pn } = readVarint(value, j); j = pn;
          if (pfn === 1) slotId = pv;
        } else if (pwt === 2) {
          let { value: pv, next: pn } = readLengthDelimited(value, j); j = pn;
          if (pfn === 2) partId = pv.toString('utf8');
          else if (pfn === 3) {
            let { ornament } = parseOrnamentWrapper(pv, 0);
            partOrnament = ornament;
          }
        } else break;
      }
      if (slotId != null && partId != null)
        parts.push({ slot: slotId, partId, ornament: partOrnament });
    } else if (fieldNumber === 3) {
      let { ornament } = parseOrnamentWrapper(value, 0);
      weaponOrnament = ornament;
    }
  }
  return { weaponId, parts, ornament: weaponOrnament };
}


// ---- Build PlayerRoleData for GetPlayerArchiveV2 response ----

function buildPlayerRoleData(archive, roleId) {
  // Case-insensitive lookup (game sends "Sniper", we may store "SNIPER")
  const lookupId =
    roleId in archive
      ? roleId
      : Object.keys(archive).find((k) => k.toLowerCase() === roleId.toLowerCase());
  const stored = lookupId ? archive[lookupId] : null;

  const out = { RoleID: roleId };
  if (!stored) return out;

  for (const key of [
    "LeftPylon",
    "RightPylon",
    "MobilityModule",
    "MeleeWeapon",
    "PrimaryWeapon",
    "SecondWeapon",
  ]) {
    const v = stored[key];
    if (typeof v === "string" && v.length > 0 && v !== "None") out[key] = v;
  }

  const skinConfig = encodeSkinConfig(stored);
  if (skinConfig) out.SkinConfig = skinConfig;

  const weaponConfig = encodeWeaponConfig(stored);
  if (weaponConfig) out.WeaponConfig = weaponConfig;

  // SkinPaint (field 10): paint/color variant string
  if (stored[SKIN_PAINT_KEY] && typeof stored[SKIN_PAINT_KEY] === "string" && stored[SKIN_PAINT_KEY] !== "None")
    out.SkinPaint = stored[SKIN_PAINT_KEY];

  return out;
}

// ---- RPC request parsers ----

function parseUpdateRoleArchiveRequest(messageBytes) {
  const buf = toBuffer(messageBytes);
  let i = 0;
  let slot = null,
    roleId = null,
    itemId = null,
    skinBase = null,
    skinOrnament = null;

  while (i < buf.length) {
    const { value: tag, next } = readVarint(buf, i);
    i = next;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x7;

    if (wireType === 0) {
      const { value, next: n } = readVarint(buf, i);
      i = n;
      if (fieldNumber === 1) slot = value;
    } else if (wireType === 2) {
      const { value, next: n } = readLengthDelimited(buf, i);
      i = n;
      const text = value.toString("utf8");
      if (fieldNumber === 2) roleId = text;
      else if (fieldNumber === 3) itemId = text;
      else if (fieldNumber === 4) {
        // Nested skin info: field 1 = base, field 2 = ornament
        let j = 0;
        while (j < value.length) {
          const { value: nTag, next: nj } = readVarint(value, j);
          j = nj;
          if ((nTag & 0x7) !== 2) break;
          const { value: nBytes, next: nk } = readLengthDelimited(value, j);
          j = nk;
          const nText = nBytes.toString("utf8");
          if (nTag >>> 3 === 1) skinBase = nText;
          else if (nTag >>> 3 === 2) skinOrnament = nText;
        }
      }
    } else if (wireType === 1) {
      i += 8;
    } else if (wireType === 5) {
      i += 4;
    } else {
      break;
    }
  }

  return { slot, roleId, itemId, skinBase, skinOrnament };
}

function parseUpdateWeaponArchiveRequest(messageBytes) {
  const buf = toBuffer(messageBytes);
  let i = 0;
  let roleId = null,
    weaponArchiveBlob = null;

  while (i < buf.length) {
    const { value: tag, next } = readVarint(buf, i);
    i = next;
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x7;

    if (wireType === 2) {
      const { value, next: n } = readLengthDelimited(buf, i);
      i = n;
      if (fieldNumber === 1) roleId = value.toString("utf8");
      else if (fieldNumber === 3) weaponArchiveBlob = value;
    } else if (wireType === 0) {
      i = readVarint(buf, i).next;
    } else if (wireType === 1) {
      i += 8;
    } else if (wireType === 5) {
      i += 4;
    } else {
      break;
    }
  }

  return { roleId, weaponArchiveBlob };
}

function extractWeaponIdFromArchiveBlob(blob) {
  if (!blob || blob.length === 0) return null;
  const { value: tag, next } = readVarint(blob, 0);
  if (tag >>> 3 === 1 && (tag & 0x7) === 2) {
    const { value } = readLengthDelimited(blob, next);
    return value.toString("utf8");
  }
  return null;
}

// ---- RPC request handlers (mutate roleArchive) ----

function handleUpdateRoleArchive(ctx) {
  const req = parseUpdateRoleArchiveRequest(ctx.messageBytes);
  if (!req.roleId) return;

  const archive = ctx.socket._roleArchive;
  if (!archive) { logger.warn(`${ctx.tag} [PERSIST] No archive on socket — gateToken not resolved`); return; }

  const entry = ensureRoleEntry(archive, req.roleId);
  const field = SLOT_TO_FIELD[req.slot];

  if (field && req.itemId) {
    entry[field] = req.itemId;
    logger.info(`${ctx.tag} [PERSIST] ${req.roleId}.${field} = ${req.itemId}`);
  } else if (req.slot === 7) {
    if (req.skinBase) entry[SKIN_BASE_KEY] = req.skinBase;
    if (req.skinOrnament) entry[SKIN_PAINT_KEY] = req.skinOrnament;
    logger.info(`${ctx.tag} [PERSIST] ${req.roleId} skin = ${req.skinBase} / ${req.skinOrnament}`);
  } else if ((req.slot === 9 || req.slot === 10) && req.itemId) {
    entry[`${COSMETIC_KEY_PREFIX}${req.slot}`] = req.itemId;
    logger.info(`${ctx.tag} [PERSIST] ${req.roleId} cosmetic #${req.slot} = ${req.itemId}`);
  }

  saveArchiveFor(ctx.socket._playerId, archive);
}

function handleUpdateWeaponArchive(ctx) {
  const req = parseUpdateWeaponArchiveRequest(ctx.messageBytes);
  if (!req.roleId || !req.weaponArchiveBlob || req.weaponArchiveBlob.length === 0) return;

  const archive = ctx.socket._roleArchive;
  if (!archive) { logger.warn("[PERSIST] No archive on socket — gateToken not resolved"); return; }

  const weaponId = extractWeaponIdFromArchiveBlob(req.weaponArchiveBlob);
  if (!weaponId) return;

  const entry = ensureRoleEntry(archive, req.roleId);
  if (!entry[WEAPON_CONFIG_KEY]) entry[WEAPON_CONFIG_KEY] = {};
  entry[WEAPON_CONFIG_KEY][weaponId] = req.weaponArchiveBlob.toString("base64");

  logger.info(
    `${ctx.tag} [PERSIST] ${req.roleId}.${weaponId} weapon archive (${req.weaponArchiveBlob.length}B)`
  );
  saveArchiveFor(ctx.socket._playerId, archive);
}

// ---- Hex diagnostic logger (disabled in production) ----

function logLoadoutHex({ rpcPath, messageId, messageBytes, frameBytes, extra }) {
  if (LOG_LEVEL_VALUE > LOG_LEVELS.debug) return; // hex diagnostics are debug-only (keeps logs/ dir small in prod)
  if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
  const day = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const logPath = path.join(LOG_DIR, `loadout-hex-${day}.log`);
  const msgBuf = toBuffer(messageBytes);
  const lines = [
    `=== ${new Date().toISOString()} ===`,
    `RPCPath: ${rpcPath}`,
    `MessageId: ${messageId}`,
    `MessageBytesLen: ${msgBuf.length}`,
    `MessageBytesHex: ${msgBuf.toString("hex")}`,
  ];
  if (frameBytes != null) {
    const fb = toBuffer(frameBytes);
    lines.push(`FrameBytesLen: ${fb.length}`, `FrameBytesHex: ${fb.toString("hex")}`);
  }
  if (extra) lines.push(`Extra: ${JSON.stringify(extra)}`);
  lines.push("");
  fs.appendFileSync(logPath, lines.join("\n"), "utf8");
}

// ---- Per-player persistence: archives loaded on gateToken handshake ----

// =============================================================================
//  HTTP (Express)
// =============================================================================

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use((req, res, next) => {
  // Skip favicon + health probes (polled constantly by the game/DLL and uptime checks).
  if (req.path === "/favicon.ico" || req.path === "/api/health") return next();

  res.on("finish", () => {
    const body =
      req.body && typeof req.body === "object" && Object.keys(req.body).length
        ? " " + JSON.stringify(req.body)
        : "";
    logger.info(`[HTTP] ${req.method} ${req.originalUrl} -> ${res.statusCode}${body}`);
  });

  logger.debug(
    `[HTTP] ${req.method} ${req.originalUrl} headers=${JSON.stringify(req.headers)} body=${JSON.stringify(req.body)}`
  );
  next();
});

app.get("/api/health", (req, res) => {
  res.status(200).json({ status: "ok" });
});

app.get("/", (req, res) => {
  res.status(200).json(matchmakingUDPServerDiscoveryPayload);
});

app.post("/recordClientStatus", (req, res) => {
  res.status(200).json({});
});

// Working connect endpoint (duplicate "//connectServer" removed — same handler)
app.post("/connectServer", (req, res) => {
  const loginToken = req.body.loginToken;
  const platform = req.body.platform;
  const playerId = req.body.playerId;
  const version = req.body.version;

  logger.info("Connection Request:", {
    platform,
    playerId,
    version,
  });

  const gateToken = crypto.randomUUID().toString();
  gateTokenToPlayerId.set(gateToken, playerId);

  res.status(200).json({
    error: 0,
    userId: playerId,
    aceId: "test",
    gateToken: gateToken,
    endpoint: RPC_ENDPOINT,
  });
});

// ---- HTTP API for DLL LoadoutFix client ----

function buildLoadoutResponse(archive, playerId) {
  const roles = {};
  for (const [roleId, stored] of Object.entries(archive)) {
    if (!stored || typeof stored !== "object") continue;
    const role = {
      primaryWeapon: stored.PrimaryWeapon || null,
      secondaryWeapon: stored.SecondWeapon || null,
      leftPylon: stored.LeftPylon || null,
      rightPylon: stored.RightPylon || null,
      meleeWeapon: stored.MeleeWeapon || null,
      mobilityModule: stored.MobilityModule || null,
      _skinToken: stored[SKIN_BASE_KEY] || null,
      _ornamentId: stored[SKIN_PAINT_KEY] || null,
      cosmetic9: stored[`${COSMETIC_KEY_PREFIX}9`] || null,
      cosmetic10: stored[`${COSMETIC_KEY_PREFIX}10`] || null,
    };
    // Convert base64 weapon configs to hex _weaponArchives format
    // AND decode the protobuf into readable JSON for DS-side consumption
    const wc = stored[WEAPON_CONFIG_KEY];
    if (wc && typeof wc === "object" && Object.keys(wc).length > 0) {
      const archives = {};
      const decoded = {};
      for (const [wid, blob] of Object.entries(wc)) {
        if (typeof blob === "string" && blob.length > 0) {
          try {
            const buf = Buffer.from(blob, "base64");
            archives[wid] = buf.toString("hex");
            decoded[wid] = decodeWeaponArchive(buf);
          } catch (_) {
            archives[wid] = blob;
            decoded[wid] = null;
          }
        }
      }
      role._weaponArchives = archives;
      role._weaponConfigs = decoded;   // decoded: { parts[], ornament }
    }
    roles[roleId] = role;
  }
  return { playerId, roles };
}

app.get("/api/loadout/:playerId", (req, res) => {
  const archive = loadArchiveFor(req.params.playerId);
  res.status(200).json(buildLoadoutResponse(archive, req.params.playerId));
});

app.get("/api/loadout/:playerId/:roleId", (req, res) => {
  const archive = loadArchiveFor(req.params.playerId);
  const roleId = req.params.roleId;
  const lookupId = roleId in archive ? roleId
    : Object.keys(archive).find((k) => k.toLowerCase() === roleId.toLowerCase());
  if (!lookupId) return res.status(404).json({ error: "Role not found" });
  const single = { [lookupId]: archive[lookupId] };
  res.status(200).json(buildLoadoutResponse(single, req.params.playerId));
});

app.put("/api/loadout/:playerId", (req, res) => {
  try {
    const playerId = req.params.playerId;
    const snapshot = req.body;
    if (!snapshot || typeof snapshot !== "object") {
      return res.status(400).json({ error: "Invalid snapshot" });
    }
    const p = path.join(DATA_DIR, `${playerId}-snapshot.json`);
    fs.writeFileSync(p, JSON.stringify(snapshot, null, 2), "utf8");
    logger.info(`[PERSIST] Snapshot saved for ${playerId}`);
    res.status(200).json({ status: "ok" });
  } catch (e) {
    logger.error("[PERSIST] Snapshot save failed:", e.message);
    res.status(500).json({ error: "Save failed" });
  }
});

// =============================================================================
//  RPC Handlers
// =============================================================================

function handleGateTokenEcho(ctx) {
  // Client echoes gateToken as RPCPath during handshake.
  // The RPCPath IS the gateToken string. Look up the playerId, load their archive.
  const token = ctx.rpcPath;
  const playerId = gateTokenToPlayerId.get(token);
  if (playerId) {
    ctx.socket._playerId = playerId;
    ctx.socket._roleArchive = loadArchiveFor(playerId);
    gateTokenToPlayerId.delete(token); // one-shot
    logger.info(`[AUTH] Socket bound to playerId=${playerId}`);
  } else {
    logger.warn(`[AUTH] Unrecognized gateToken: ${token.substring(0, 36)}`);
  }
  ctx.socket.write(ctx.frameBytes);
}

function handleUpdateRoleArchiveV2(ctx) {
  handleUpdateRoleArchive(ctx);
  logLoadoutHex({
    rpcPath: ctx.rpcPath,
    messageId: ctx.messageId,
    messageBytes: ctx.messageBytes,
    frameBytes: ctx.frameBytes,
  });
  const resp = encodeStatusCodeResponse(
    "./game/proto/Response/UpdateRoleArchiveV2.proto",
    "ProjectBoundary.UpdateRoleArchiveV2Response"
  );
  sendEncodedProtoResponse(ctx.socket, ctx.messageId, ctx.rpcPath, resp);
}

function handleUpdateWeaponArchiveV2(ctx) {
  handleUpdateWeaponArchive(ctx);
  logLoadoutHex({
    rpcPath: ctx.rpcPath,
    messageId: ctx.messageId,
    messageBytes: ctx.messageBytes,
    frameBytes: ctx.frameBytes,
  });
  // Same status-code proto as UpdateRoleArchiveV2 (intentional / matches original)
  const resp = encodeStatusCodeResponse(
    "./game/proto/Response/UpdateRoleArchiveV2.proto",
    "ProjectBoundary.UpdateRoleArchiveV2Response"
  );
  sendEncodedProtoResponse(ctx.socket, ctx.messageId, ctx.rpcPath, resp);
}

function handleGetPlayerArchiveV2(ctx) {
  logger.info(`${ctx.tag} [RECV] GetPlayerArchiveV2`);

  const reqObj = decodeRequestMessage(
    "./game/proto/Request/GetPlayerArchiveV2Request.proto",
    "ProjectBoundary.GetPlayerArchiveV2Request",
    ctx.messageBytes
  );
  const roleIds = reqObj.RoleIDs || [];
  const archive = ctx.socket._roleArchive || {};
  const playerId = ctx.socket._playerId || "unknown";

  logLoadoutHex({
    rpcPath: ctx.rpcPath,
    messageId: ctx.messageId,
    messageBytes: ctx.messageBytes,
    frameBytes: ctx.frameBytes,
    extra: { RoleIDs: roleIds, playerId },
  });

  // Codec sub_1409D3CE0: field 1 (0x0a) = repeated RoleArchiveDataV2, field 2 (0x10) = varint.
  // Field 2 omitted when PlayerLevel=0 (proto3 default).
  const responseObj = { PlayerRoleDatas: [], PlayerLevel: 0 };
  for (const roleId of roleIds) {
    responseObj.PlayerRoleDatas.push(buildPlayerRoleData(archive, roleId));
  }

  const responseBytes = encodeProtoMessage(
    "./game/proto/Response/GetPlayerArchiveV2Response.proto",
    "ProjectBoundary.GetPlayerArchiveV2Response",
    responseObj
  );

  const responseFrame = wrapResponse(ctx.messageId, ctx.rpcPath, responseBytes);
  logLoadoutHex({
    rpcPath: ctx.rpcPath + " (response)",
    messageId: ctx.messageId,
    messageBytes: responseBytes,
    frameBytes: responseFrame,
    extra: { RoleIDs: roleIds, playerId, source: getArchivePath(playerId) },
  });
  ctx.socket.write(responseFrame);
}

function handleQueryAssets(ctx) {
  logger.info(`${ctx.tag} [RECV] Query Assets`);
  const responseObj = { ItemDatas: [], ItemCount: 0 };
  try {
    const allItems = Object.keys(
      JSON.parse(fs.readFileSync("./game/definitions/DT_ItemType.json", "utf8"))[0]["Rows"]
    );
    for (const item of allItems) {
      responseObj.ItemDatas.push({ ItemId: item, Unknown1: 1, Unknown2: 1, Unknown3: 1 });
    }
    responseObj.ItemCount = responseObj.ItemDatas.length;
  } catch (e) {
    logger.error("[QueryAssets] Failed:", e.message);
  }
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryAssetsResponse.proto",
    "ProjectBoundary.QueryAssetsResponse",
    responseObj
  );
}

function handleQueryNotification(ctx) {
  const reqObj = decodeRequestMessage(
    "./game/proto/Request/QueryNotificationRequest.proto",
    "ProjectBoundary.QueryNotificationRequest",
    ctx.messageBytes
  );
  const platform = reqObj.Platform;
  const languageCode = reqObj.LanguageCode;

  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryNotificationResponse.proto",
    "ProjectBoundary.QueryNotificationResponse",
    {
      Unknown: 0,
      Notifications: [
        buildNotification(
          "4/01/2026 Patchnotes",
          PATCHNOTES_4012026_TEXT,
          "",
          languageCode,
          platform,
          "America/New_York"
        ),
        buildNotification(
          "3/31/2026 Patchnotes",
          PATCHNOTES_3312026_TEXT,
          "",
          languageCode,
          platform,
          "America/New_York"
        ),
        buildNotification(
          "Project Rebound Alpha",
          ALPHA_TEXT,
          "",
          languageCode,
          platform,
          "America/New_York"
        ),
      ],
    }
  );
}

function handleCreateParty(ctx) {
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/CreatePartyResponse.proto",
    "ProjectBoundary.CreatePartyResponse",
    { StatusCode: 0, PartyId: crypto.randomUUID().toString(), PartyMembers: [TEMP_USER_ID] }
  );
}

function handlePartyReady(ctx) {
  const reqObj = decodeRequestMessage(
    "./game/proto/Request/PartyReadyRequest.proto",
    "ProjectBoundary.PartyReadyRequest",
    ctx.messageBytes
  );
  const partyId = reqObj.PartyId; // parsed for parity with original (unused)
  void partyId;
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/PartyReadyResponse.proto",
    "ProjectBoundary.PartyReadyResponse",
    { StatusCode: 0 }
  );
}

function handlePartyGet(_ctx) {
  // no-op
}

function handleTextFilter(_ctx) {
  // no-op
}

function handleSetPresence(ctx) {
  const reqObj = decodeRequestMessage(
    "./game/proto/Request/SetPartyPresenceRequest.proto",
    "ProjectBoundary.SetPartyPresenceRequest",
    ctx.messageBytes
  );
  partyPresence = reqObj.Presence;
  logger.info(`${ctx.tag} [PARTY] Presence => ${partyPresence}`);
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/SetPartyPresenceResponse.proto",
    "ProjectBoundary.SetPartyPresenceResponse",
    { StatusCode: 0 }
  );
}

function handleQueryPresence(ctx) {
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryPartyPresenceResponse.proto",
    "ProjectBoundary.QueryPartyPresenceResponse",
    {
      StatusCode: 0,
      PartyMembers: [{ UserId: TEMP_USER_ID, Status: partyPresence }],
    }
  );
}

function handleQueryUnityMatchmakingRegion(ctx) {
  logger.info(`${ctx.tag} [RECV] Query Matchmaking Region`);
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryMatchmakingRegionResponse.proto",
    "ProjectBoundary.QueryMatchmakingRegionResponse",
    { StatusCode: 0, Regions: buildRegionList() }
  );
}

function handleStartUnityMatchmaking(ctx) {
  logger.info(`${ctx.tag} [RECV] Start Matchmaking`);
  const reqObj = decodeRequestMessage(
    "./game/proto/Request/StartMatchmakingRequest.proto",
    "ProjectBoundary.StartMatchmakingRequest",
    ctx.messageBytes
  );
  const userIdToMatchmake = reqObj.Payload.MatchmakingRequestorUserId; // parsed for parity
  void userIdToMatchmake;
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/StartMatchmakingResponse.proto",
    "ProjectBoundary.StartMatchmakingResponse",
    { StatusCode: 0 }
  );
}

function handleGetDataStatisticsInfo(ctx) {
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/GetDataStatisticsInfoResponse.proto",
    "ProjectBoundary.GetDataStatisticsInfoResponse",
    { StatusCode: 0, Datapoints: [] }
  );
}

function handleQueryPlayList(ctx) {
  logger.info(`${ctx.tag} [RECV] Query Playlists`);
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryPlaylistResponse.proto",
    "ProjectBoundary.QueryPlaylistResponse",
    { StatusCode: 0, PlaylistsJSON: JSON.stringify(PLAYLISTS_JSON) }
  );
}

function handleQueryCurrency(ctx) {
  sendProtoResponse(
    ctx.socket,
    ctx.messageId,
    ctx.rpcPath,
    "./game/proto/Response/QueryCurrencyResponse.proto",
    "ProjectBoundary.QueryCurrencyResponse",
    { CurrencyA: 0, CurrencyB: 0, CurrencyC: 0, CurrencyD: 0, CurrencyE: 0 }
  );
}

const rpcHandlers = {
  "/assets.Assets/UpdateRoleArchiveV2": handleUpdateRoleArchiveV2,
  "/assets.Assets/UpdateWeaponArchiveV2": handleUpdateWeaponArchiveV2,
  "/assets.Assets/GetPlayerArchiveV2": handleGetPlayerArchiveV2,
  "/assets.Assets/QueryAssets": handleQueryAssets,
  "/notification.Notification/QueryNotification": handleQueryNotification,
  "/party.party/Create": handleCreateParty,
  "/party.party/Ready": handlePartyReady,
  "/party.party/Get": handlePartyGet,
  "/chat.chat/TextFilter": handleTextFilter,
  "/party.party/SetPresence": handleSetPresence,
  "/party.party/QueryPresence": handleQueryPresence,
  "/matchmaking.Matchmaking/QueryUnityMatchmakingRegion": handleQueryUnityMatchmakingRegion,
  "/matchmaking.Matchmaking/StartUnityMatchmaking": handleStartUnityMatchmaking,
  "/playerdata.PlayerDataClient/GetDataStatisticsInfo": handleGetDataStatisticsInfo,
  "/matchmaking.Matchmaking/QueryPlayList": handleQueryPlayList,
  "/profile.Profile/QueryCurrency": handleQueryCurrency,
};

// Client telemetry / fire-and-forget paths we intentionally don't answer.
// Suppressed at info level so they don't flood the console during normal play.
const IGNORED_RPC_PATHS = [
  "/mission.Mission/",
  "/event.Event/",
  "/eventtracking.EventTracking/",
];
const ignoredRpcNoticed = new Set();

function dispatchRpc(ctx) {
  ctx.tag = ctx.socket._playerId
    ? `[player ${ctx.socket._playerId}]`
    : `[${ctx.socket.remoteAddress}:${ctx.socket.remotePort}]`;

  // GateToken echo: dynamic UUID tokens resolve the socket's player identity
  if (gateTokenToPlayerId.has(ctx.rpcPath)) {
    handleGateTokenEcho(ctx);
    return;
  }

  const handler = rpcHandlers[ctx.rpcPath];
  if (!handler) {
    const ignored = IGNORED_RPC_PATHS.some((p) => ctx.rpcPath.startsWith(p));
    if (ignored) {
      if (!ignoredRpcNoticed.has(ctx.rpcPath)) {
        ignoredRpcNoticed.add(ctx.rpcPath);
        logger.debug(`${ctx.tag} [RPC] ignoring known no-op ${ctx.rpcPath} (suppressed at info)`);
      }
      return;
    }
    logger.warn(`${ctx.tag} [RPC] Unhandled message ${ctx.rpcPath} (MessageId ${ctx.messageId})`);
    logLoadoutHex({
      rpcPath: ctx.rpcPath,
      messageId: ctx.messageId,
      messageBytes: ctx.messageBytes,
      frameBytes: ctx.frameBytes,
      extra: { UNHANDLED: true },
    });
    return;
  }

  try {
    handler(ctx);
  } catch (err) {
    logger.error(
      `${ctx.tag} [RPC] Handler failed path=${ctx.rpcPath} MessageId=${ctx.messageId}:`,
      err && err.stack ? err.stack : err
    );
  }
}

// =============================================================================
//  TCP RPC Server (port RPC_PORT)
// =============================================================================

function processTcpFrame(socket, frameBytes) {
  // Early connection echo: length=2, payload="//"
  if (frameBytes.length === 6 && frameBytes.toString("hex") === TCP_HANDSHAKE_ECHO_HEX) {
    socket.write(frameBytes);
    return;
  }

  const requestWrapperType = loadProtoType(
    PROTO_REQUEST_WRAPPER,
    "ProjectBoundary.RequestWrapper"
  );

  let requestWrapper;
  try {
    requestWrapper = requestWrapperType.decode(frameBytes.subarray(TCP_LENGTH_HEADER_SIZE));
  } catch (_) {
    return;
  }

  if (requestWrapper == undefined) return;

  const requestObj = requestWrapperType.toObject(requestWrapper, objectOptions);
  const messageId = requestObj.MessageId;
  const rpcPath = requestObj.RPCPath;
  const messageBytes = requestObj.Message;

  dispatchRpc({
    socket,
    messageId,
    rpcPath,
    messageBytes,
    frameBytes,
  });
}

/**
 * Proper length-prefixed receive buffer:
 *   chunk → append → while complete frame → parse → leave remainder
 * Handles fragmentation, coalescing, empty payloads, and bad length headers.
 */
function onTcpData(socket, chunk) {
  socket._recvBuffer = Buffer.concat([socket._recvBuffer, chunk]);

  while (true) {
    if (socket._recvBuffer.length < TCP_LENGTH_HEADER_SIZE) {
      return;
    }

    const payloadLength = socket._recvBuffer.readUInt32BE(0);

    // Malformed / absurd length — drop header byte and try to resync without crashing
    if (
      !Number.isFinite(payloadLength) ||
      payloadLength < 0 ||
      payloadLength > MAX_TCP_FRAME_PAYLOAD
    ) {
      logger.warn(
        `[TCP] Malformed length header (${payloadLength}) from ${socket.remoteAddress}:${socket.remotePort}; discarding 1 byte`
      );
      socket._recvBuffer = socket._recvBuffer.subarray(1);
      continue;
    }

    const frameLength = TCP_LENGTH_HEADER_SIZE + payloadLength;
    if (socket._recvBuffer.length < frameLength) {
      return; // wait for rest of frame
    }

    const frameBytes = socket._recvBuffer.subarray(0, frameLength);
    socket._recvBuffer = socket._recvBuffer.subarray(frameLength);

    try {
      processTcpFrame(socket, frameBytes);
    } catch (err) {
      logger.error(
        `[TCP] Frame processing error from ${socket.remoteAddress}:${socket.remotePort}:`,
        err && err.stack ? err.stack : err
      );
    }
  }
}

const server = net.createServer((socket) => {
  logger.info(`[RPC] client connected from ${socket.remoteAddress}:${socket.remotePort}`);

  socket._recvBuffer = Buffer.alloc(0);

  socket.on("data", (chunk) => {
    try {
      onTcpData(socket, chunk);
    } catch (err) {
      logger.error(
        `[TCP] Receive buffer error from ${socket.remoteAddress}:${socket.remotePort}:`,
        err && err.stack ? err.stack : err
      );
    }
  });

  socket.on("end", () =>
    logger.info(
      `[RPC] client disconnected ${socket.remoteAddress}:${socket.remotePort}` +
        (socket._playerId ? ` (player ${socket._playerId})` : "")
    )
  );
  socket.on("error", (err) =>
    logger.error(`[RPC] socket error ${socket.remoteAddress}:${socket.remotePort}:`, err.message)
  );
});

server.on("error", (err) => {
  logger.error(`[RPC] server error: ${err.message}`);
});

// =============================================================================
//  UDP Matchmaking (port MATCHMAKING_PORT)
// =============================================================================

const matchmakingUDPServer = udp.createSocket("udp4");

matchmakingUDPServer.on("error", (error) => {
  logger.error("[MM] UDP error:", error.toString());
  matchmakingUDPServer.close();
});

matchmakingUDPServer.on("close", () => {
  logger.info("[MM] UDP socket closed");
});

matchmakingUDPServer.on("message", (message, info) => {
  if (message[0] == QOS_REQUEST_BYTE) {
    const header = Buffer.alloc(3);
    header[0] = QOS_RESPONSE_BYTE;
    header[1] = 0x00;
    const resp = Buffer.concat([header, message.subarray(11)]);
    matchmakingUDPServer.send(resp, info.port, info.address, () => {});
    logger.debug(`[MM] QoS echo -> ${info.address}:${info.port} (${message.length}B)`);
  } else {
    logger.warn(`[MM] Unknown UDP message from ${info.address}:${info.port}: ${message.toString("hex")}`);
  }
});

matchmakingUDPServer.on("listening", () => {
  logger.info(`[MM] UDP bound on :${MATCHMAKING_PORT}`);
});

const matchmakingTCPServer = net.createServer((socket) => {
  logger.info(`[MM] TCP client connected from ${socket.remoteAddress}:${socket.remotePort}`);
  socket.on("data", (rawdata) => {
    logger.debug(`[MM] TCP data (${rawdata.length}B): ${rawdata.toString("hex").slice(0, 128)}`);
  });
  // Internet scanners (Shodan/Censys/etc.) hit :9000/TCP and reset abruptly.
  // Without this listener, the socket 'error' event becomes an uncaught
  // exception and crashes the whole metaserver (pm2 restarts it in a loop).
  socket.on("error", (err) => {
    logger.debug(`[MM] TCP socket error ${socket.remoteAddress}:${socket.remotePort}: ${err.message}`);
  });
  socket.on("close", () => {
    logger.debug(`[MM] TCP client disconnected ${socket.remoteAddress}:${socket.remotePort}`);
  });
});
matchmakingTCPServer.on("error", (err) => {
  logger.error(`[MM] TCP server error: ${err.message}`);
});

// =============================================================================
//  Startup
// =============================================================================

// Safety net: a single bad connection must never take the whole metaserver down.
// Log-and-continue (instead of the default crash) so pm2 doesn't restart-loop.
process.on("uncaughtException", (err) => {
  logger.error("[FATAL] uncaughtException:", err && err.stack ? err.stack : err);
});
process.on("unhandledRejection", (reason) => {
  logger.error("[FATAL] unhandledRejection:", reason);
});

app.listen(HTTP_PORT, () => {
  logger.info(`[HTTP] listening on :${HTTP_PORT}`);
  server.listen(RPC_PORT, () => {
    logger.info(`[RPC]  listening on :${RPC_PORT}`);
    matchmakingUDPServer.bind(MATCHMAKING_PORT);
    matchmakingTCPServer.listen(MATCHMAKING_PORT);
  });
});
