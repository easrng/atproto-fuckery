import * as cbor from "@atcute/cbor";
import * as CID from "@atcute/cid";
import { fromBase64Url, toBase32, toBase64Url } from "@atcute/multibase";
import * as t from "./types.ts";
import {
  GenesisHashError,
  ImproperOperationError,
  InvalidSignatureError,
  MisorderedOperationError,
} from "./error.ts";
import { PrivateKey, verifySigWithDidKey } from "@atcute/crypto";

const toSha256 = async (input: Uint8Array): Promise<Uint8Array> => {
	const digest = await crypto.subtle.digest('SHA-256', input);
	return new Uint8Array(digest);
};

export const didForCreateOp = async (op: t.CompatibleOp) => {
  const hashOfGenesis = await toSha256(cbor.encode(op));
  const hashB32 = toBase32(hashOfGenesis);
  const truncated = hashB32.slice(0, 24);
  return `did:plc:${truncated}`;
};

// Operations formatting
// ---------------------------

export const formatAtprotoOp = (opts: {
  signingKey: string;
  handle: string;
  pds: string;
  rotationKeys: string[];
  prev: CID.Cid | null;
}): t.UnsignedOperation => {
  return {
    type: "plc_operation",
    verificationMethods: {
      atproto: opts.signingKey,
    },
    rotationKeys: opts.rotationKeys,
    alsoKnownAs: [ensureAtprotoPrefix(opts.handle)],
    services: {
      atproto_pds: {
        type: "AtprotoPersonalDataServer",
        endpoint: ensureHttpPrefix(opts.pds),
      },
    },
    prev: opts.prev?.toString() ?? null,
  };
};

export const atprotoOp = async (opts: {
  signingKey: string;
  handle: string;
  pds: string;
  rotationKeys: string[];
  prev: CID.Cid | null;
  signer: PrivateKey;
}) => {
  return addSignature(formatAtprotoOp(opts), opts.signer);
};

export const createOp = async (opts: {
  signingKey: string;
  handle: string;
  pds: string;
  rotationKeys: string[];
  signer: PrivateKey;
}): Promise<{ op: t.Operation; did: string }> => {
  const op = await atprotoOp({ ...opts, prev: null });
  const did = await didForCreateOp(op);
  return { op, did };
};

export const createUpdateOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  fn: (normalized: t.UnsignedOperation) => Omit<t.UnsignedOperation, "prev">
): Promise<t.Operation> => {
  const prev = CID.create(0x71, cbor.encode(lastOp));
  // omit sig so it doesn't accidentally make its way into the next operation
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { sig: _, ...normalized } = normalizeOp(lastOp);
  const unsigned = await fn(normalized);
  return addSignature(
    {
      ...unsigned,
      prev: prev.toString(),
    },
    signer
  );
};

export const createAtprotoUpdateOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  opts: Partial<{
    signingKey: string;
    handle: string;
    pds: string;
    rotationKeys: string[];
  }>
) => {
  return createUpdateOp(lastOp, signer, (normalized) => {
    const updated = { ...normalized };
    if (opts.signingKey) {
      updated.verificationMethods = {
        ...normalized.verificationMethods,
        atproto: opts.signingKey,
      };
    }
    if (opts.handle) {
      const formatted = ensureAtprotoPrefix(opts.handle);
      const handleI = normalized.alsoKnownAs.findIndex((h) =>
        h.startsWith("at://")
      );
      if (handleI < 0) {
        updated.alsoKnownAs = [formatted, ...normalized.alsoKnownAs];
      } else {
        updated.alsoKnownAs = [
          ...normalized.alsoKnownAs.slice(0, handleI),
          formatted,
          ...normalized.alsoKnownAs.slice(handleI + 1),
        ];
      }
    }
    if (opts.pds) {
      const formatted = ensureHttpPrefix(opts.pds);
      updated.services = {
        ...normalized.services,
        atproto_pds: {
          type: "AtprotoPersonalDataServer",
          endpoint: formatted,
        },
      };
    }
    if (opts.rotationKeys) {
      updated.rotationKeys = opts.rotationKeys;
    }
    return updated;
  });
};

export const updateAtprotoKeyOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  signingKey: string
): Promise<t.Operation> => {
  return createAtprotoUpdateOp(lastOp, signer, { signingKey });
};

export const updateHandleOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  handle: string
): Promise<t.Operation> => {
  return createAtprotoUpdateOp(lastOp, signer, { handle });
};

export const updatePdsOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  pds: string
): Promise<t.Operation> => {
  return createAtprotoUpdateOp(lastOp, signer, { pds });
};

export const updateRotationKeysOp = async (
  lastOp: t.CompatibleOp,
  signer: PrivateKey,
  rotationKeys: string[]
): Promise<t.Operation> => {
  return createAtprotoUpdateOp(lastOp, signer, { rotationKeys });
};

export const tombstoneOp = async (
  prev: CID.Cid,
  key: PrivateKey
): Promise<t.Tombstone> => {
  return addSignature(
    {
      type: "plc_tombstone",
      prev: prev.toString(),
    },
    key
  );
};

// Signing operations
// ---------------------------

export const addSignature = async <T extends Record<string, unknown>>(
  object: T,
  key: PrivateKey
): Promise<T & { sig: string }> => {
  const data = new Uint8Array(cbor.encode(object));
  const sig = await key.sign(data);
  return {
    ...object,
    sig: toBase64Url(sig),
  };
};

export const signOperation = async (
  op: t.UnsignedOperation,
  signingKey: PrivateKey
): Promise<t.Operation> => {
  return addSignature(op, signingKey);
};

// Backwards compatibility
// ---------------------------

export const deprecatedSignCreate = async (
  op: t.UnsignedCreateOpV1,
  signingKey: PrivateKey
): Promise<t.CreateOpV1> => {
  return addSignature(op, signingKey);
};

export const normalizeOp = (op: t.CompatibleOp): t.Operation => {
  if (op.type === "plc_operation") {
    return op;
  }
  return {
    type: "plc_operation",
    verificationMethods: {
      atproto: op.signingKey,
    },
    rotationKeys: [op.recoveryKey, op.signingKey],
    alsoKnownAs: [ensureAtprotoPrefix(op.handle)],
    services: {
      atproto_pds: {
        type: "AtprotoPersonalDataServer",
        endpoint: ensureHttpPrefix(op.service),
      },
    },
    prev: op.prev,
    sig: op.sig,
  };
};

// Verifying operations/signatures
// ---------------------------

export const assureValidCreationOp = async (
  did: string,
  op: t.CompatibleOpOrTombstone
): Promise<t.DocumentData> => {
  if (op.type === "plc_tombstone") {
    throw new MisorderedOperationError();
  }
  const normalized = normalizeOp(op);
  await assureValidSig(normalized.rotationKeys, op);
  const expectedDid = await didForCreateOp(op);
  if (expectedDid !== did) {
    throw new GenesisHashError(expectedDid);
  }
  if (op.prev !== null) {
    throw new ImproperOperationError("expected null prev on create", op);
  }
  const { verificationMethods, rotationKeys, alsoKnownAs, services } =
    normalized;
  return { did, verificationMethods, rotationKeys, alsoKnownAs, services };
};

export const assureValidSig = async (
  allowedDidKeys: string[],
  op: t.CompatibleOpOrTombstone
): Promise<string> => {
  const { sig, ...opData } = op;
  if (sig.endsWith("=")) {
    throw new InvalidSignatureError(op);
  }
  const sigBytes = fromBase64Url(sig);
  const dataBytes = new Uint8Array(cbor.encode(opData));
  for (const didKey of allowedDidKeys) {
    const isValid = await verifySigWithDidKey(didKey, sigBytes, dataBytes);
    if (isValid) {
      return didKey;
    }
  }
  throw new InvalidSignatureError(op);
};

// Util
// ---------------------------

export const ensureHttpPrefix = (str: string): string => {
  if (str.startsWith("http://") || str.startsWith("https://")) {
    return str;
  }
  return `https://${str}`;
};

export const ensureAtprotoPrefix = (str: string): string => {
  if (str.startsWith("at://")) {
    return str;
  }
  const stripped = str.replace("http://", "").replace("https://", "");
  return `at://${stripped}`;
};
