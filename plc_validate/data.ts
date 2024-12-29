const SECOND = 1e3;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;

import * as CID from "@atcute/cid";
import * as cbor from "@atcute/cbor";
import * as t from "./types.ts";
import {
  assureValidCreationOp,
  assureValidSig,
  normalizeOp,
} from "./operations.ts";
import {
  LateRecoveryError,
  MisorderedOperationError,
} from "./error.ts";

export const assureValidNextOp = async (
  did: string,
  ops: t.IndexedOperation[],
  proposed: t.IndexedOperation
): Promise<{
  nullified: string[];
  prev: CID.Cid | null;
  ops: t.IndexedOperation[];
}> => {
  // special case if account creation
  if (ops.length === 0) {
    await assureValidCreationOp(did, proposed.operation);
    return { nullified: [], prev: null, ops: [proposed] };
  }

  const proposedPrev = proposed.operation.prev
    ? CID.fromString(proposed.operation.prev)
    : undefined;
  if (!proposedPrev) {
    throw new MisorderedOperationError();
  }

  const indexOfPrev = ops.findIndex(
    (op) => CID.toString(proposedPrev) === op.cid
  );
  if (indexOfPrev < 0) {
    throw new MisorderedOperationError();
  }

  // if we are forking history, these are the ops still in the proposed canonical history
  const opsInHistory = ops.slice(0, indexOfPrev + 1);
  const nullified = ops.slice(indexOfPrev + 1);
  const lastOp = opsInHistory.at(-1);
  if (!lastOp) {
    throw new MisorderedOperationError();
  }
  if (lastOp.operation.type === "plc_tombstone") {
    throw new MisorderedOperationError();
  }
  const lastOpNormalized = normalizeOp(lastOp.operation);
  const firstNullified = nullified[0];

  // if this does not involve nullification
  if (!firstNullified) {
    await assureValidSig(lastOpNormalized.rotationKeys, proposed.operation);
    return { nullified: [], prev: proposedPrev, ops: [...ops, proposed] };
  }

  const disputedSigner = await assureValidSig(
    lastOpNormalized.rotationKeys,
    firstNullified.operation
  );

  const indexOfSigner = lastOpNormalized.rotationKeys.indexOf(disputedSigner);
  const morePowerfulKeys = lastOpNormalized.rotationKeys.slice(
    0,
    indexOfSigner
  );

  await assureValidSig(morePowerfulKeys, proposed.operation);

  // recovery key gets a 72hr window to do historical re-wrties
  if (nullified.length > 0) {
    const RECOVERY_WINDOW = 72 * HOUR;
    const timeLapsed =
      Date.parse(proposed.createdAt) - Date.parse(firstNullified.createdAt);
    if (timeLapsed > RECOVERY_WINDOW) {
      throw new LateRecoveryError(timeLapsed);
    }
  }

  return {
    nullified: nullified.map((op) => op.cid),
    prev: proposedPrev,
    ops: [...opsInHistory, proposed],
  };
};

export const validateOperationLog = async (
  did: string,
  ops: t.IndexedOperation[]
): Promise<t.DocumentData | null> => {
  let history: t.IndexedOperation[] = [];
  for (const op of ops) {
    ({ ops: history } = await assureValidNextOp(did, history, op));
  }
  return opToData(did, history.at(-1)!.operation);
};

export const opToData = (
  did: string,
  op: t.CompatibleOpOrTombstone
): t.DocumentData | null => {
  if (op.type === "plc_tombstone") {
    return null;
  }
  const { verificationMethods, rotationKeys, alsoKnownAs, services } =
    normalizeOp(op);
  return { did, verificationMethods, rotationKeys, alsoKnownAs, services };
};

export const getLastOpWithCid = async (
  ops: t.CompatibleOpOrTombstone[]
): Promise<{ op: t.CompatibleOpOrTombstone; cid: CID.Cid }> => {
  const op = ops.at(-1);
  if (!op) {
    throw new Error("log is empty");
  }
  const cid = await CID.create(0x71, cbor.encode(op));
  return { op, cid };
};
