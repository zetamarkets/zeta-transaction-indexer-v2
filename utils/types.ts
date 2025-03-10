import { Kind } from "@zetamarkets/sdk/dist/types";
import { TransactionSignature } from "@solana/web3.js";

export interface EventQueueHeader {
  head: number;
  count: number;
  seqNum: number;
}

export interface ConfirmedSignatureInfoShort {
  signature: TransactionSignature | undefined;
  blockTime: number | undefined;
  slot: number | undefined;
}

export interface Trade {
  seq_num: number;
  timestamp: number;
  owner_pub_key: string;
  expiry_series_index: number;
  market_index: number;
  expiry_timestamp: number;
  strike: number;
  kind: Kind;
  is_fill: boolean;
  is_maker: boolean;
  is_bid: boolean;
  price: number;
  size: number;
}

export interface Pricing {
  timestamp: number;
  slot?: number;
  expiry_series_index: number;
  expiry_timestamp: number;
  market_index: number;
  strike: number;
  kind: Kind;
  theo: number;
  delta: number;
  sigma: number;
  vega: number;
}

export interface Surface {
  timestamp: number;
  slot?: number;
  expiry_series_index: number;
  expiry_timestamp: number;
  vol_surface: number[];
  nodes: number[];
  interest_rate: number;
}

export interface ZetaTransaction {
  transaction_id: string;
  block_timestamp: number;
  slot: number;
  is_successful: boolean;
  fee: number;
  accounts: string[];
  instructions: Instruction[];
  log_messages: string[];
  fetch_timestamp: number;
}

export interface TableIndices {
  earliest: string | undefined;
  latest: string | undefined;
}

export interface Instruction {
  name: string;
  instruction: Object;
  named_accounts: Object;
  program_id: string;
}
