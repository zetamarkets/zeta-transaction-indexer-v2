import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
let ddb = new AWS.DynamoDB(AWSOptions);


export const writeBackfillCheckpoint = (
  tableName: string,
  incomplete_top: string | undefined,
  bottom_sig: string | undefined,
  backfill_complete: boolean | false
  ) => {
    if (incomplete_top == undefined) {
      incomplete_top = ""
    }
    if (bottom_sig == undefined) {
      bottom_sig = ""
    }
    var params = {
      TableName: tableName,
      Item: {
        id: { S: `${process.env.NETWORK!}-backfill-checkpoint` },
        incomplete_top: { S: incomplete_top },
        bottom_sig: { S: bottom_sig },
        backfill_complete: { BOOL: backfill_complete },
      },
    };
    
    // Call DynamoDB to add the item to the table
    ddb.putItem(params, function (err, data) {
      if (err) {
        console.error("[ERROR] Backfill Checkpoint Write Error", err);
      } else {
        console.info("[INFO] Backfill Checkpoint successfully written.");
      }
    });
  };


export const readBackfillCheckpoint = async (tableName: string) => {
  var params = {
    TableName: tableName,
    Key: {
      id: { S: `${process.env.NETWORK!}-backfill-checkpoint` },
    },
    ConsistentRead: true,
  };
  
  // Call DynamoDB to read the item from the table
  let q = ddb.getItem(params);
  try {
    const r = await q.promise();
    if (r.Item) {
      let incomplete_top = r.Item.incomplete_top.S;
      let bottom_sig = r.Item.bottom.S;
      let backfill_complete = r.Item.backfill_complete.BOOL;
      if (incomplete_top == "") {
        incomplete_top = undefined;
      }
      if (bottom_sig == "") {
        bottom_sig = undefined;
      }
      console.info(`[INFO] Reading Backfill checkpoint - Incomplete Top: ${incomplete_top}, Bottom Sig: ${bottom_sig}, Backfill complete: ${backfill_complete}`);
      return {
        incomplete_top,
        bottom_sig,
        backfill_complete,
      };
    } else {
      console.warn(`[WARN] No Backfill Checkpoint Found`);
      return { incomplete_top: undefined, bottom_sig: undefined, backfill_complete: false };
    }
  } catch (error) {
    throw error;
  }
};


export const writeFrontfillCheckpoint = (
  tableName: string,
  new_top: string | undefined,
  new_top_block_time: number | undefined,
  new_top_slot: number | undefined,
  ) => {
    if (new_top == undefined) {
      new_top = ""
    }
    if (new_top_block_time == undefined) {
      new_top_block_time = 0
    }
    if (new_top_slot == undefined) {
      new_top_slot = 0
    }
    var params = {
      TableName: tableName,
      Item: {
        id: { S: `${process.env.NETWORK!}-frontfill-checkpoint` },
        old_top: { S: new_top },
        old_top_block_time: { N: String(new_top_block_time) },
        old_top_slot: { N: String(new_top_slot) },
      },
    };
    
    // Call DynamoDB to add the item to the table
    ddb.putItem(params, function (err, data) {
      if (err) {
        console.error("[ERROR] Frontfill Checkpoint Write Error", err);
      } else {
        console.info("[INFO] Frontfill Checkpoint successfully written.");
      }
    });
  };


export const readFrontfillCheckpoint = async (tableName: string) => {
  var params = {
    TableName: tableName,
    Key: {
      id: { S: `${process.env.NETWORK!}-frontfill-checkpoint` },
    },
    ConsistentRead: true,
  };

  // Call DynamoDB to read the item from the table
  let q = ddb.getItem(params);
  try {
    const r = await q.promise();
    if (r.Item) {
      let old_top = r.Item.old_top.S;
      if (old_top == "") {
        old_top = undefined;
      }
      var old_top_block_time = Number(r.Item.old_top_block_time.N);
      var old_top_slot = Number(r.Item.old_top_slot.N);
      if (old_top_block_time === 0) {
        old_top_block_time = undefined;
      }
      if (old_top_slot === 0) {
        old_top_slot = undefined;
      }
      console.info(`[INFO] Reading Frontfill checkpoint - Signature: ${old_top}, BlockTime: ${old_top_block_time}, Slot: ${old_top_slot}`);
      return { old_top, old_top_block_time, old_top_slot };
    } else {
      console.warn(`[WARN] No Frontfill Checkpoint Found`);
      return { old_top: undefined, old_top_block_time: undefined, old_top_slot: undefined };
    }
  } catch (error) {
    throw error;
  }
};