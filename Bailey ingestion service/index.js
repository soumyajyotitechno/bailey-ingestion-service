

const axios = require("axios");

const pg = require("pg");



//require('dotenv').config();

const https = require("https");
const { applyPaymentExclusion } = require("./payment_exclusion_rule");

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 5,      // down from 25 — you don't need that many for sequential calls
  maxFreeSockets: 2,
  timeout: 8000,
});

const apiClient = axios.create({
  httpsAgent,
  timeout: 30000,
});

const { Pool } = pg;

console.log("httpsAgent config:", { keepAlive: httpsAgent.keepAlive, maxSockets: httpsAgent.maxSockets });

// ---- Bailey-specific constants for the aura_transactions_raw payload ----
const TRX_SOURCE_ID = process.env.AURA_TRX_SOURCE_ID || "IMPOS";
const IS_TESTING =0
const PARTNER_NAME ='Bailey Brewing Co.'
const PARTNER_ID='IMPOS-202612'
const ENABLE_CARD_MATCHING=false
const pool = new Pool({
  host: process.env.HOST,
  database: process.env.DBNAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: process.env.PORT,
  max: 3,
});


// ---------------------------------------------------------------------
// Separate Aura raw-transaction processor. Runs independently of the
// IMPOS pull/ingest -- scans bailey_transactions for rows that are
// approved (per isApprovedForValue) and not yet pushed to
// aura_transactions_raw, resolves a member, inserts, and marks processed.
// ---------------------------------------------------------------------

async function safeQuery(client, sql, params = []) {
  try {
    return await client.query(sql, params);
  } catch (err) {
    console.error("Query failed:", err.message);

    if (Array.isArray(params)) {
      params.forEach((p, i) => {
        if (p === null || p === undefined) {
          console.error(`Param $${i+1}: NULL`);
        } else if (typeof p === "string") {
          console.error(`Param $${i+1}: length=${p.length}, value="${p}"`);
        } else {
          console.error(`Param $${i+1}: type=${typeof p}, value=${p}`);
        }
      });
    } else {
      console.error("Params were not an array:", params);
    }

    throw err; // rethrow so your rollback still happens
  }
}

async function getUnprocessedApprovedOrders(client, limit = 200) {
  const { rows } = await safeQuery(client,
    `
    SELECT *
    FROM bailey_transactions
    WHERE trx_raw_processed = false
      AND void_state    <> 'voided'
     AND payment_state <> 'unpaid'
       AND void_state    <> 'refunded'
       AND order_state   <> 'voided'
    ORDER BY pos_created_at asc
    LIMIT $1
    `,
    [limit]
  );
  return rows;
}

async function getPaymentsForOrder(client, transactionId, siteId) {
  const { rows } = await safeQuery(client,
    `SELECT * FROM bailey_payments WHERE transaction_id = $1 AND site_id = $2`,
    [transactionId, siteId]
  );
  return rows;
}

// Rebuilds the minimal "order shape" resolveMemberIdForOrder /
// insertAuraTransactionRaw expect, from DB rows instead of the raw
// IMPOS payload. NOTE: checkTotal is multiplied back to cents here
// (bailey_transactions.check_total is stored in dollars) so downstream
// functions -- written against the raw IMPOS shape -- don't need to
// change their /100 math.
function buildOrderShapeFromDb(txRow, payments) {
  return {
    id: txRow.transaction_id,
    siteId: txRow.site_id,
    checkTotal: Math.round(Number(txRow.check_total) * 100),
    posCreatedAt: txRow.pos_created_at,
    posUpdatedAt: txRow.pos_updated_at,
    orderState: txRow.order_state,
    paymentState: txRow.payment_state,
    voidState: txRow.void_state,
    member: txRow.member ?? null, // assumes `member` column stores the raw {cardNumber,...} JSON
    transactions: payments.map((p) => ({
      id: p.id,
      transactionState: p.transaction_state,
      paymentState: p.payment_state,
      memberID: p.member_id,
      detail: { pan: p.pan_masked },
       // detail: { pan: '433273XXXXXX1234' },
      
    })),
  };
}



async function processAuraTransactions(pool, batchSize = 200, rotateEvery = 20) {
  let processed = 0;
  let failed = 0;

  let client = await pool.connect();
  try {
    const candidates = await getUnprocessedApprovedOrders(client, batchSize);

    for (let i = 0; i < candidates.length; i++) {
      // Rotate connection periodically so no single socket accumulates
      // listeners across hundreds of sequential queries.
      if (i > 0 && i % rotateEvery === 0) {
        client.release();
        client = await pool.connect();
      }

      const txRow = candidates[i];
      try {
        await client.query("BEGIN");

        const payments = await getPaymentsForOrder(client, txRow.transaction_id, txRow.site_id);
        const o = buildOrderShapeFromDb(txRow, payments);
        const checkTotal = safeDivideBy100(o.checkTotal);        

        if (checkTotal <= 0) {
          console.warn(`Order ${o.id}: checkTotal is zero or negative (${checkTotal}), skipping Aura insert.`);
          await safeQuery(client,
            `UPDATE bailey_transactions SET trx_raw_processed = true, trx_raw_processed_at = now()
             WHERE transaction_id = $1 AND site_id = $2`,
            [txRow.transaction_id, txRow.site_id]
          );
          await client.query("COMMIT");
          continue;
        }

        const memberId = await resolveMemberIdForOrder(client, o);


     

        if (memberId) {
          o.merchantId = "IMPOS";
          o.member_id = memberId;
          o.trx_amount_modified = checkTotal;
          o.partner_name = PARTNER_NAME;
          o.partner_id = PARTNER_ID;

          //=====apply payment method exclusion rules=====
          const excludeAmount = await applyPaymentExclusion(client, payments, PARTNER_ID);
          const trx_amount_modified = Math.max(0, checkTotal - excludeAmount);
           o.trx_amount_modified = trx_amount_modified;
          //=========END apply payment method exclusion rules=========
          console.log(`Check total: ${checkTotal}, Excluded: ${excludeAmount}, Modified total: ${trx_amount_modified}`);

        

          await insertAuraTransactionRaw(client, o);
          processed++;
        }

        await safeQuery(client,
          `UPDATE bailey_transactions
           SET trx_raw_processed = true, trx_raw_processed_at = now()
           WHERE transaction_id = $1 AND site_id = $2`,
          [txRow.transaction_id, txRow.site_id]
        );

        await client.query("COMMIT");
      } catch (err) {
        failed++;
        console.error(`Aura processing failed for order ${txRow.transaction_id}:`, err.message);
        try { await client.query("ROLLBACK"); } catch (e) { console.error("Rollback failed:", e.message); }
      }
    }
  } finally {
    client.release();
  }

  return { processed, failed };
}

const getMemberIdByExternalId = async (client, referral_id) => {

  const { rows } = await safeQuery(client,
    `SELECT member_id FROM aura_customer WHERE referral_id = $1`,
    [referral_id]
  ); 
  return rows[0]?.member_id || null;
};



function safeDivideBy100(value) {
  if (value == null) return 0;           // null/undefined → 0
  const num = Number(value);
  if (isNaN(num)) return 0;              // non-numeric → 0
  return Math.max(0, num / 100);         // clamp negatives to 0
}
async function resolveMemberIdForOrder(client, order) {
  const orderId = order.id;
  console.log("cardNumber before==>",order.member)
  let memberData = order.member;
  if (typeof memberData === 'string') {
    try {
      memberData = JSON.parse(memberData);
    } catch (e) {
      console.error("Failed to parse order.member as JSON:", e, order.member);
      memberData = null;
    }
  }
  
  const cardNumber = memberData?.cardNumber ? String(memberData.cardNumber).trim() : null;
  
  // --- Priority 1: order.member.cardNumber -> referral_id lookup ---
  // Covers partial redemption, full redemption, and Aura barcode scan --
  // all three just mean cardNumber is populated, so no branching needed.
  console.log("cardNumber ==>",cardNumber)
  if (cardNumber) {
    const memberId = await getMemberIdByExternalId(client, cardNumber);
    if (memberId) return memberId;

    console.warn(
      `Order ${orderId}: order.member.cardNumber "${cardNumber}" present but no match via getMemberIdByExternalId`
    );
  }

  // --- Priority 2: order.member.cardNumber empty -> fall back to transaction card ---
  // --- Priority 2: order.member.cardNumber empty -> fall back to transaction card ---
  if (ENABLE_CARD_MATCHING === true)
    {
      console.log("ENABLE_CARD_MATCHING ",ENABLE_CARD_MATCHING)
      const qualifyingTx = Array.isArray(order.transactions)
        ? order.transactions.filter(
            (t) => t?.transactionState === "active" && t?.paymentState === "paid"
          )
        : [];
  
      for (const tx of qualifyingTx) {
        if (!tx.detail?.pan) continue;
  
        const cardInfo = extractCardInfo(tx.detail.pan);
        
        if (!cardInfo) {
          console.warn(`Order ${orderId}, tx ${tx.id}: invalid card info, skipping`);
          continue;
        }
      
        const memberId = await getMemberId(cardInfo.bin6, cardInfo.lastFour);
        if (memberId){
          console.log("cardInfo ===>",cardInfo)
          return memberId;
        }  
      
      }
    }

  return null;
}


async function insertAuraTransactionRaw(client, o) {
  // NOTE: previously this whole function body was wrapped in its own
  // try/catch that swallowed all errors (just console.log(error)).
  // That meant a failed aura insert would still be counted as a
  // success by the caller (auraInserts++) and would NOT trigger the
  // per-order rollback. Errors now propagate to the caller's existing
  // per-order try/catch, which already handles ROLLBACK + logging.

  // Check if transaction already exists
  const { rows } = await safeQuery(client,
    `
        SELECT 1
        FROM aura_transactions_raw
        WHERE trx_source_id = $1
          AND original_trx_id = $2
          AND partner_id = $3
        LIMIT 1
        `,
    [
      o.merchantId,
      o.id,
      o.partner_id
    ]
  );
  const mappedStatus =
    o.orderState === "completed"
      ? "approved"
      : o.orderState === "voided"
        ? "cancelled"
        : o.orderState === "refunded"
          ? "refunded"
          : "pending";

  if (rows.length > 0) {
    console.log("Updating existing transaction...");

    await safeQuery(client,
      `
        UPDATE aura_transactions_raw
        SET
            member_id = $4,
            trx_date_time = $5,
            trx_amount = $6,
            trx_amount_modified = $7,
            created_date_time = $8,
            partner_name = $9,
            is_transfer = $11,
            trx_status = $10,
            updated_at = now()
        WHERE trx_source_id = $1
          AND original_trx_id = $2
          AND partner_id = $3
        `,
      [
        o.merchantId,
        o.id,
        o.partner_id,
        o.member_id,
        o.posCreatedAt,
        Number(o.checkTotal) / 100,
        o.trx_amount_modified,
        o.posUpdatedAt,
        o.partner_name,
        mappedStatus,
        IS_TESTING   // $11 — now a real boolean
      ]
    );

  } else {
    console.log("Inserting new transaction...");

    await safeQuery(client,
      `
        INSERT INTO aura_transactions_raw (
            is_test,
            trx_source_id,
            original_trx_id,
            member_id,
            trx_date_time,
            trx_amount,
            trx_amount_modified,
            created_date_time,
            trx_status,
            partner_id,
            partner_name,
            is_transfer
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
        )
        `,
      [
        IS_TESTING,
        o.merchantId,
        o.id,
        o.member_id,
        o.posCreatedAt,
        Number(o.checkTotal) / 100,
        o.trx_amount_modified,
        o.posUpdatedAt,
        mappedStatus,   // <-- parameterized
        o.partner_id,
        o.partner_name,
        IS_TESTING
      ]
    );
  }
}
function extractCardInfo(pan) {
  if (!pan || pan.length < 10) return null;
  return {
    bin6: pan.slice(0, 6),
    lastFour: pan.slice(-4),
  };
}

const getMemberId = async (firstSix, lastFour) => {
  
  const response = await apiClient.post(`https://7ivzcls5ab.execute-api.ap-southeast-2.amazonaws.com/default/card_matching_service`,
    {
      firstSix,
      lastFour
    });
  return response.data?.member_id || null;
};



exports.handler = async (event) => {
  try {
    const batchSize = 500;
   
    if(IS_TESTING===1)
    {
      console.log('runing in test mode')
      return;
    }
    console.log('runing in live mode')
    const result = await processAuraTransactions(pool, batchSize);
    console.log(`Aura processing run:`, result);
    return result;
  } catch (err) {
    console.error("Aura processing Lambda failed:", err);
    throw err;
  }
};

// (async()=>{
//  await this.handler()
// })()
