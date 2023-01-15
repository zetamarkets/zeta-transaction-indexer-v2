import {
  ConfirmedSignatureInfo,
  Connection,
  PublicKey,
  TransactionSignature,
} from "@solana/web3.js";
import { sendMessage } from "./utils/sqs";
import { SolanaRPC } from "./utils/rpc";
import { MAX_SIGNATURE_BATCH_SIZE, DEBUG_MODE } from "./utils/constants";
import { sleep } from "@zetamarkets/sdk/dist/utils";
import {
  readFrontfillCheckpoint,
  readBackfillCheckpoint,
  writeFrontfillCheckpoint,
  writeBackfillCheckpoint,
} from "./utils/dynamodb";
import { ConfirmedSignatureInfoShort } from "./utils/types";

let rpc = new SolanaRPC(
  process.env.RPC_URL_1,
  process.env.RPC_URL_2,
  "finalized"
);


async function indexSignaturesForAddress(
  address: PublicKey,
  before?: TransactionSignature,
  until?: TransactionSignature,
  backfill_complete?: boolean,
  old_top_block_time?: number,
  old_top_slot?: number,
) {
  let sigs: ConfirmedSignatureInfo[];
  let top: ConfirmedSignatureInfoShort = {
    signature: before,
    blockTime: undefined,
    slot: undefined,
  };
  let bottom: ConfirmedSignatureInfoShort = {
    signature: until,
    blockTime: old_top_block_time,
    slot: old_top_slot,
  };
  if (!backfill_complete && before === undefined && until === undefined) {
    console.info("Not top defined in backfill, write front fill checkpoint immediately");
    var backfill_frontfill_checkpoint_write = true;
  } else {
    var backfill_frontfill_checkpoint_write = false;
  }
  let firstFlag = true;
  let newTop: ConfirmedSignatureInfoShort;
  let prev_bottom: ConfirmedSignatureInfoShort = {
    signature: undefined,
    blockTime: undefined,
    slot: undefined,
  };
  let simultaneousBottoms = new Object();
  simultaneousBottoms["blockTime"] = undefined;
  simultaneousBottoms["slot"] = undefined;
  do {
    sigs = await rpc.getSignaturesForAddressWithRetries(address, {
      before: top.signature,
      until: bottom.signature,
      limit: 1000,
    });
    sigs.reverse();

    // Retry Logic if theres an empty sig list retry c times with some sleep and backoff/wait
    let c = 1;
    let backoff = 1.35;
    let sleepTime = 1000;
    while ((sigs.length < 1) && c < 5) {
      console.warn(`[WARN] No signatures found, retrying in ${sleepTime}ms...`);
      await sleep(sleepTime);
      console.warn(`[WARN] No signatures found, retrying ${c} time(s)...`);
      sigs = await rpc.getSignaturesForAddressWithRetries(address, {
        before: top.signature,
        until: bottom.signature,
        limit: 1000,
      });
      sigs.reverse();
      c += 1;
      sleepTime *= backoff;
    }

    // ======== FRONTFILL ONLY ========
    // If we are frontfilling need to check for same blocktimed txs from the bottom
    if (sigs.length > 0 && backfill_complete) {
      // Settign temp bottom (since its possible they are removed from the list)
      let temp_bottom = {
        signature: sigs[0].signature,
        blockTime: sigs[0].blockTime,
        slot: sigs[0].slot,
      };

      // Checking if the bottom of the current run matches the blocktime and slot of the final bottom (until)
      console.info(`[INFO] Previous Bottom: ${prev_bottom.signature}, Blocktime: ${prev_bottom.blockTime}, Slot: ${prev_bottom.slot}`);
      console.info(`[INFO] Current Bottom : ${temp_bottom.signature}, Blocktime: ${temp_bottom.blockTime}, Slot: ${temp_bottom.slot}`);
      console.info(`[INFO] Final Bottom   : ${until}, Blocktime: ${old_top_block_time}, Slot: ${old_top_slot}`);
      
      // Checking for the case where the last two+ txs have the same blocktime and slot
      let checkFlag = false;
      if (sigs.length > 1) {
        if (temp_bottom.blockTime == sigs[1].blockTime && temp_bottom.slot == sigs[1].slot) {
          console.info(`[INFO] Check Flag: True`);
          checkFlag = true;
        }
      }

      // If the bottom of the current run matches the blocktime and slot of the final bottom (until)
      if ((temp_bottom.blockTime === old_top_block_time && temp_bottom.slot === old_top_slot) || (temp_bottom.blockTime === prev_bottom.blockTime && temp_bottom.slot === prev_bottom.slot) || (checkFlag)) {
        console.info("[INFO] Found bottom of frontfill with MATCHING blocktime and slot!");
        let simultaneousBottomIndexes = [];
        if ((temp_bottom.blockTime != simultaneousBottoms["blockTime"] && temp_bottom.slot != simultaneousBottoms["slot"]) && (temp_bottom.blockTime != old_top_block_time && temp_bottom.slot != old_top_slot)) {
          // If the recent bottom isnt the same as the previous bottom or the final bottom we can reset the simultaneousBottoms
          console.info('[INFO] Resetting simultaneousBottoms');
          simultaneousBottoms = new Object();
          simultaneousBottoms["blockTime"] = temp_bottom.blockTime;
          simultaneousBottoms["slot"] = temp_bottom.slot;
        }

        for (var i = 0; i < sigs.length; i += 1) {
          // Checking from the bottom upwards which txs are the exact same blocktime and slot
          if (sigs[i].blockTime === old_top_block_time && sigs[i].slot === old_top_slot || sigs[i].blockTime === prev_bottom.blockTime && sigs[i].slot === temp_bottom.slot) {
            if (simultaneousBottoms[sigs[i].signature] === undefined) {
              // If this signature has not yet been recorded, add to the object 
              simultaneousBottoms[sigs[i].signature] = sigs[i];
            } else if (((sigs[i].blockTime === old_top_block_time && sigs[i].slot === old_top_slot) || (sigs[i].blockTime === prev_bottom.blockTime && sigs[i].slot === temp_bottom.slot)) && simultaneousBottoms[sigs[i].signature] != undefined) {
              // If this signature has already been recorded, record the index for splicing
              simultaneousBottomIndexes.push(i);
            } else {
              // Once the above conditions are not met break, since all following txs are later/diff slot
              break;
            }
          }
        }

        console.info(`[INFO] Bottom TXs / Repeated Indexes Length: ${Object.keys(simultaneousBottoms).length - 2} / ${simultaneousBottomIndexes.length}`);
        // Reverse the index list so its largest first
        simultaneousBottomIndexes.reverse();
        // Splice out the signatures that have already been recorded (largest index first so smaller indexes remain valid)
        console.info(`[INFO] SIGS LENGTH (BEFORE): ${sigs.length}`);
        for (var j = 0; j < simultaneousBottomIndexes.length; j += 1) {
          // Secondary Check
          if (simultaneousBottoms[sigs[simultaneousBottomIndexes[j]].signature] != undefined) {
            // Splice out the simultaneous bottom txs
            sigs.splice(simultaneousBottomIndexes[j], 1);
          }
        }
        console.info(`[INFO] SIGS LENGTH (AFTER): ${sigs.length}`);
      }
      // Rememeber the previous bottom
      prev_bottom = temp_bottom;
    }

    // ======== BOTH FRONTFILL & BACKFILL ========
    // Checking if any sigs were returned
    if (sigs.length > 0) {
      // Set top and bottom of current run
      top = {
        signature: sigs[sigs.length - 1].signature,
        blockTime: sigs[sigs.length - 1].blockTime,
        slot: sigs[sigs.length - 1].slot,
      };
      bottom = {
        signature: sigs[0].signature,
        blockTime: sigs[0].blockTime,
        slot: sigs[0].slot,
      };
      // For first iteration
      if (firstFlag) {
        // Start process (1st iteration) - write new top
        newTop = top;
        firstFlag = false;
      }

      // infoging the bottom (oldest tx) to the top (most recent tx) of the current run
      console.info(
        `[INFO] Indexed [${sigs.length}] txs: ${sigs[0].signature} (${new Date(
          sigs[0].blockTime * 1000
        ).toISOString()}) - ${sigs[sigs.length - 1].signature} (${new Date(
          sigs[sigs.length - 1].blockTime * 1000
        ).toISOString()})`
      );

      // Write Frontfill checkpoint immediately after first run
      if (backfill_frontfill_checkpoint_write) {
        console.info(`[INFO] Writing Frontfill Checkpoint IMMEDIATELY after first backfill run`);
        writeFrontfillCheckpoint(
          process.env.CHECKPOINT_TABLE_NAME,
          newTop.signature,
          newTop.blockTime,
          newTop.slot,
        );
        backfill_frontfill_checkpoint_write = false;
      }
  
      // Push Messages to SQS
      if (!DEBUG_MODE) {
        sendMessage(
          sigs.map((s) => s.signature),
          process.env.SQS_QUEUE_URL
        );
      }

      // Update Local Pointers
      top = bottom;
      bottom = {
        signature: until,
        blockTime: old_top_block_time,
        slot: old_top_slot,
      };

      if (!backfill_complete) {
        // Update DynamoDB Checkpoint
        writeBackfillCheckpoint(
          process.env.CHECKPOINT_TABLE_NAME,
          top.signature,
          undefined, // can to top or undefined both work here... (more useful for specific backfilling scenarios)
          false,
        );
      }

    } else {
      // No more signatures to index
      console.info(`[INFO] Signature list is empty ${sigs}`);
      // Never occurs on the first run (have to check again if there are 0 sigs on subsequent run to confirm)
      if (!firstFlag) {
        // Only writes frontfill checkpoint at the end in a front fill scenario
        if (backfill_complete) {
          writeFrontfillCheckpoint(
            process.env.CHECKPOINT_TABLE_NAME,
            newTop.signature,
            newTop.blockTime,
            newTop.slot,
          );
        } else {
          // Mark backfill as complete (true) (old top should be written on first run of backfill)
          writeBackfillCheckpoint(
            process.env.CHECKPOINT_TABLE_NAME,
            undefined,
            undefined, // can to top or undefined both work here... (more useful for specific backfilling scenarios)
            true,
          );

          // Write Frontfill Checkpoint IF ts undefined for some reason (cases where defined backfill is run without ff checkpoint existing)
          let { old_top, old_top_block_time, old_top_slot } = await readFrontfillCheckpoint(
            process.env.CHECKPOINT_TABLE_NAME
          );
          if (old_top == undefined) {
            console.log("[INFO] Writing FF checkpoint after backfill complete since none was found...")
            writeFrontfillCheckpoint(
              process.env.CHECKPOINT_TABLE_NAME,
              newTop.signature,
              newTop.blockTime,
              newTop.slot,
            );
          }
        }
      } else {
        console.info('[INFO] First Flag is true AND no new sigs found, skipping write checkpoints.');
      }
      break;
    }

    if (sigs[0] == undefined || !sigs[sigs.length - 1] == undefined) {
      console.error("[ERROR] Null signature detected");
    }

  } while (true);
  return { bottom: bottom, top: top };
}


export const refreshConnection = async () => {
  rpc.connection = new Connection(rpc.nodeUrl, rpc.commitmentOrConfig);
};


const main = async () => {
  if (DEBUG_MODE) {
    console.info("[INFO] Running in debug mode, will not push to AWS buckets");
  }

  // Periodically read checkpoints to confirm there are changes occuring
  let prev_backfill = {
    incomplete_top: undefined,
    bottom_sig: undefined,
    backfill_complete: undefined
  };

  let prev_frontfill = {
    old_top: undefined,
    old_top_block_time: undefined,
    old_top_slot: undefined
  };

  setInterval(async () => {
    let new_backfill = await readBackfillCheckpoint(process.env.CHECKPOINT_TABLE_NAME);
    let new_frontfill = await readFrontfillCheckpoint(process.env.CHECKPOINT_TABLE_NAME);
    if (
      new_backfill.incomplete_top === prev_backfill.incomplete_top && 
      new_backfill.bottom_sig === prev_backfill.bottom_sig &&
      new_backfill.backfill_complete === prev_backfill.backfill_complete &&
      new_frontfill.old_top === prev_frontfill.old_top &&
      new_frontfill.old_top_block_time === prev_frontfill.old_top_block_time &&
      new_frontfill.old_top_slot === prev_frontfill.old_top_slot) {
      console.error("[ERROR] No change in checkpoints");
      // Kill container task
      process.exit(1);
    } else {
      console.info("[INFO] Checkpoints have changed, continuing...");
      prev_backfill = new_backfill;
      prev_frontfill = new_frontfill;
    }
  }, 1000 * 60 * 30); // Check every 30 minutes

  // Periodic refresh of rpc connection to prevent hangups
  setInterval(async () => {
    console.info("%c[INFO] Refreshing rpc connection", "color: cyan");
    refreshConnection();
  }, 1000 * 60 * 5); // Refresh every 5 minutes

  if (process.env.RESET === "true") {
    console.info("[INFO] Resetting checkpoints...");
    writeFrontfillCheckpoint(
      process.env.CHECKPOINT_TABLE_NAME,
      undefined,
      undefined,
      undefined,
      );
    writeBackfillCheckpoint(
      process.env.CHECKPOINT_TABLE_NAME,
      undefined,
      undefined,
      false,
    );
  }

  let top: ConfirmedSignatureInfoShort;
  let bottom: ConfirmedSignatureInfoShort;

  // Start Indexing
  while (true) {
    // get pointers from storage
    let { incomplete_top, bottom_sig, backfill_complete } = await readBackfillCheckpoint(
      process.env.CHECKPOINT_TABLE_NAME
    );
    console.info(`[INFO] Incomplete Top: ${incomplete_top}, Bottom: ${bottom_sig}, Backfill Complete: ${backfill_complete}`);


    if (process.env.FRONTFILL_ONLY === "true") {
      // Frontfill only mode
      console.info("[INFO] Running in frontfill only mode...");
      backfill_complete = true;
    }

    if (backfill_complete) {
      // Frontfill

      // Checking where the old 'top' was...
      let { old_top, old_top_block_time, old_top_slot } = await readFrontfillCheckpoint(
        process.env.CHECKPOINT_TABLE_NAME
      );

      if (!old_top) {
        // Old top is undefined something is wrong, proceed to backfill
        console.error("[ERROR] Backfilling Required, Setting Backfill to false")
        let { incomplete_top, bottom_sig, backfill_complete } = await readBackfillCheckpoint(
          process.env.CHECKPOINT_TABLE_NAME
        );
        writeBackfillCheckpoint( process.env.CHECKPOINT_TABLE_NAME, incomplete_top, bottom_sig, false);
      } else {
          // ...and indexing from the front to the old top
          console.info(`[INFO] Frontfilling: Indexing up until: ${old_top}`);

          ({ bottom, top } = await indexSignaturesForAddress(
            new PublicKey(process.env.PROGRAM_ID),
            undefined,
            old_top,
            backfill_complete,
            old_top_block_time,
            old_top_slot,
          ));
      }
    } else {
      // Backfill
      console.info(`[INFO] No prior data, proceeding to backfill. Starting at: ${incomplete_top}`);
      
      ({ bottom, top } = await indexSignaturesForAddress(
        new PublicKey(process.env.PROGRAM_ID),
        incomplete_top,
        bottom_sig,
        backfill_complete,
        undefined,
        undefined,
      ));
      console.info(`[INFO] Backfill Complete!`);
    }

    console.info("[INFO] Indexing up to date, waiting a few seconds...");
    await sleep(10000); // 10 seconds
  }
};

main().catch(console.error.bind(console));
