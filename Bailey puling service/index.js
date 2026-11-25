const axios = require("axios");
const FormDataNode = require("form-data");
const pg = require("pg");
// const { applySalesRule } = require("./salesItemRule");
//require('dotenv').config();

const https = require("https");

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 25,
  maxFreeSockets: 10,
  timeout: 30000,
});

const apiClient = axios.create({
  httpsAgent,
  timeout: 30000,
});

const { Pool } = pg;
const OVERLAP_MS =  30000; // 30 seconds in ms

const PAGE_LIMIT = 50;
const IS_TESTING = 1; //make it 0 when 

const SITES=[
  65,66
]

const PARTNER_NAME ='Bailey Brewing Co.'
const PARTNER_ID='IMPOS-202612'
const EXCLUDE_MEU_TRX = false

const pool = new Pool({
  host: process.env.HOST,
  database: process.env.DBNAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: process.env.PORT,
  max: 3,
});



// ---------------------------------------------------------------------
// 1. Token generation (username/password Basic auth)
// ---------------------------------------------------------------------
async function getAuthToken() {
  const basic = Buffer.from(
    `${process.env.IMPOS_USERNAME}:${process.env.IMPOS_PASSWORD}`
  ).toString("base64");
  console.log(basic)
  const form = new FormDataNode();
  form.append("grant_type", "client_credentials");
  form.append("scope", "openid email");

  const res = await axios.post(process.env.IMPOS_TOKEN_URL, form, {
    headers: {
      Authorization: `Basic ${basic}`,
      ...form.getHeaders(),
    },
    timeout: 10000,
  });
  if (!res.data?.access_token) throw new Error("Token response missing access_token");
  return res.data.access_token;
}
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

// ---------------------------------------------------------------------
// 2. Pull orders for a site, filtered by UpdatedFrom/UpdatedTo
//
//    IMPORTANT: the API has two independent date filter pairs:
//      - From/To            -> filters on CreatedAt (when first ingested)
//      - UpdatedFrom/UpdatedTo -> filters on UpdatedAt (when last modified)
//    We deliberately filter on UpdatedAt: an order can be created at
//    11pm and not close out (paid/void/refunded) until 1am. Its
//    CreatedAt stays "yesterday" forever, so a From/To window scoped to
//    "today" would never see it, even though it just changed. Filtering
//    on UpdatedAt means any order that changed inside the window is
//    caught regardless of when it was originally opened.
// ---------------------------------------------------------------------
async function fetchOrders(siteId,paramJson, token) {
  let page = 1;
  const all = [];
  let totalCount = null;

  while (true) {
    const res = await apiClient.get(`${process.env.IMPOS_BASE_URL}/v1/sites/${siteId}/orders`, {
      params: { Page: page, Limit: PAGE_LIMIT, ...paramJson },
      headers: { accept: "text/plain", Authorization: `Bearer ${token}` },
      // timeout: 20000,
    });

    const records = res.data?.records ?? [];
    const meta = res.data?.metadata;

    all.push(...records);
    console.log(
      `Fetched page ${meta?.currentPage ?? page}/${meta?.totalPages ?? "?"}: ${records.length} orders`
    );

    if (!meta) {
      // Defensive fallback if metadata is ever missing from the response
      if (records.length === 0) break;
      page += 1;
      continue;
    }

    totalCount = meta.totalCount;
    if (!meta.hasNext) break;
    page += 1;
    // FIX: original code had an unconditional `break` here right after
    // incrementing `page`, so only the first page (max PAGE_LIMIT
    // orders) was ever fetched regardless of hasNext. With 10k+
    // orders/site/day that silently dropped almost everything. Removed
    // so the loop now runs until hasNext is false.
  }

  if (totalCount !== null && all.length !== totalCount) {
    console.warn(`Fetched ${all.length} records but API reported totalCount=${totalCount}`);
  }

  return all;
}

// ---------------------------------------------------------------------
// 3a. Card info extraction
// ---------------------------------------------------------------------
function extractCardInfo(pan) {
  if (!pan || pan.length < 10) return null;
  return {
    bin6: pan.slice(0, 6),
    lastFour: pan.slice(-4),
  };
}

// ---------------------------------------------------------------------
// 4. Ingest order into bailey_transactions / bailey_sales / bailey_payments
//    (simple upsert, one row at a time -- fine for small date-range pulls)
// ---------------------------------------------------------------------
async function ingestOrder(client, o, siteId) {
  await safeQuery(client,
    `
    INSERT INTO bailey_transactions (
      transaction_id, site_id, pos_id, pos_check_date, pos_check_no, check_type,
      order_no, order_name, order_type, covers, table_name, table_floor,
      surcharge, round_total, check_total, check_gst,
      order_state, payment_state, discount_state, void_state, cash_clearing_unit,
      notes, version, member, meal_status, source, discount_status, void_status,
      action_log_id, performed_at, station_id, station_identifier, station_name, location_name,
      employee_identifier, employee_display_name, authorised_by,
      pos_created_at, pos_updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,$11,$12,
      $13,$14,$15,$16,
      $17,$18,$19,$20,$21,
      $22,$23,$24,$25,$26,$27,$28,
      $29,$30,$31,$32,$33,$34,
      $35,$36,$37,
      $38,$39
    )
    ON CONFLICT (transaction_id,site_id) DO UPDATE SET
      order_state       = EXCLUDED.order_state,
      payment_state     = EXCLUDED.payment_state,
      void_state        = EXCLUDED.void_state,
      discount_state    = EXCLUDED.discount_state,
      check_total       = EXCLUDED.check_total,
      check_gst         = EXCLUDED.check_gst,
      surcharge         = EXCLUDED.surcharge,
      round_total       = EXCLUDED.round_total,
      notes             = EXCLUDED.notes,
      version           = EXCLUDED.version,
      pos_updated_at    = EXCLUDED.pos_updated_at,
      created_at        = now()
    WHERE EXCLUDED.pos_updated_at >= bailey_transactions.pos_updated_at
    `,
    [
      o.id, siteId, o.posId, o.posCheckDate, o.posCheckNo, o.checkType,
      o.orderNo, o.orderName, o.orderType, o.covers, o.table?.tableName, o.table?.tableFloor,
      o.surcharge != null ? Math.max(0, Number(o.surcharge) / 100) : 0,
      o.roundTotal != null ? Math.max(0, Number(o.roundTotal) / 100) : 0,
      o.checkTotal != null ? Math.max(0, Number(o.checkTotal) / 100) : 0,
      o.checkGst != null ? Math.max(0, Number(o.checkGst) / 100) : 0,
      o.orderState, o.paymentState, o.discountState, o.voidState, o.cashClearingUnit,
      o.notes, o.version, o.member, o.mealStatus, o.source, o.discountStatus, o.voidStatus,
      o.actionLog?.id, o.actionLog?.performedAt, o.actionLog?.station?.id, o.actionLog?.station?.identifier, o.actionLog?.station?.stationName, o.actionLog?.station?.locationName,
      o.actionLog?.employee?.identifier, o.actionLog?.employee?.displayName, o.actionLog?.authorisedBy,
      o.posCreatedAt, o.posUpdatedAt
    ]
  );

  for (const it of o.items ?? []) {
    await safeQuery(client,
      `
      INSERT INTO bailey_sales (
        id, transaction_id, site_id, pos_item_id, menu_item_id, product_plu,
        name1, quantity, unit_price_base, item_price, department_name,
        void_status, pos_updated_at,
        pos_created_at, previous_quantity, parent_item_id, price_level, price_period,
        receipt_print_flag, kitchen_print_status, course, course_name,
        unit_cost_base, has_stock_link, tare_weight, set_menu_header_id, scanned_barcode,
        is_used_in_deal_group, is_deal_group_header, item_deal_group_parent_id,
        department_id, department_version, department_category_id, category_id,
        discount_status, source_application_id, source_name, source_external_uuid,
        action_log_id, action_log_performed_at, station_id, station_identifier,
        station_name, station_location_name, employee_identifier, employee_display_name,
        authorised_by, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,
        $7,$8,$9,$10,$11,
        $12,$13,
        $14,$15,$16,$17,$18,
        $19,$20,$21,$22,
        $23,$24,$25,$26,$27,
        $28,$29,$30,
        $31,$32,$33,$34,
        $35,$36,$37,$38,
        $39,$40,$41,$42,
        $43,$44,$45,$46,
        $47,$48
      )
      ON CONFLICT (id,transaction_id,site_id) DO UPDATE SET
        item_price          = EXCLUDED.item_price,
        void_status         = EXCLUDED.void_status,
        pos_updated_at      = EXCLUDED.pos_updated_at,
        discount_status     = EXCLUDED.discount_status,
        updated_at          = EXCLUDED.updated_at,
        created_at          = now()
      WHERE EXCLUDED.pos_updated_at >= bailey_sales.pos_updated_at
      `,
      [
        it.id, o.id, siteId, it.posItemId, it.menuItemId, it.productPLU,
        it.name1,
        it.quantity != null ? Math.max(0, Number(it.quantity)) : 0, // ✅ quantity >= 0
        it.unitPriceBase != null ? Math.max(0, Number(it.unitPriceBase) / 100) : 0, // ✅ unit_price_base >= 0
        it.itemPrice != null ? Math.max(0, Number(it.itemPrice) / 100) : 0, // ✅ item_price >= 0
        it.department?.name1, it.voidStatus, it.posUpdatedAt ?? o.posUpdatedAt,
        it.posCreatedAt, it.previousQuantity, it.parentItemId, it.priceLevel, it.pricePeriod,
        it.receiptPrintFlag, it.kitchenPrintStatus, it.course?.course, it.course?.courseName,
        it.unitCostBase != null ? Math.max(0, Number(it.unitCostBase) / 100) : 0, // ✅ unit_cost_base >= 0
        it.hasStockLink, it.tareWeight, it.setMenuHeaderId, it.scannedBarcode,
        it.isUsedInDealGroup, it.isDealGroupHeader, it.itemDealGroupParentId,
        it.department?.id, it.department?.version, it.department?.categoryId, it.category?.id,
        it.discountStatus, it.source?.applicationId, it.source?.name, it.source?.externalUUID,
        it.actionLog?.id, it.actionLog?.performedAt, it.actionLog?.station?.id, it.actionLog?.station?.identifier,
        it.actionLog?.station?.stationName, it.actionLog?.station?.locationName,
        it.actionLog?.employee?.identifier, it.actionLog?.employee?.displayName,
        it.actionLog?.authorisedBy, it.updatedAt
      ]
    );
    
  }

for (const tx of o.transactions ?? []) {
  await safeQuery(client,
    `
    INSERT INTO bailey_payments (
      id, transaction_id, site_id, pos_transaction_id, payment_total,
      tips, payment_state, transaction_state, method_name, card_type,
      pan_masked, pos_created_at, pos_updated_at,
      index, amount_redeemed, change_amount, member_id,
      surcharge_amount, surcharge_rate, void_state, void_status,
      linked_transaction_id, linked_transaction,
      method_id, method_identifier, method_type, method_type_identifier,
      detail_id, merchant_id, terminal_id, reference_id, stan, rrn, auth_code,
      system_transaction_date, currency, transaction_type,
      source_application_id, source_name, source_external_uuid,
      version, action_log_id, action_log_performed_at,
      station_id, station_identifier, station_name, station_location_name,
      employee_identifier, employee_display_name, authorised_by
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,$8,$9,$10,
      $11,$12,$13,
      $14,$15,$16,$17,
      $18,$19,$20,$21,
      $22,$23,
      $24,$25,$26,$27,
      $28,$29,$30,$31,$32,$33,$34,
      $35,$36,$37,
      $38,$39,$40,
      $41,$42,$43,
      $44,$45,$46,$47,
      $48,$49,$50
    )
    ON CONFLICT (id,transaction_id,site_id) DO UPDATE SET
      payment_state       = EXCLUDED.payment_state,
      transaction_state   = EXCLUDED.transaction_state,
      pos_updated_at      = EXCLUDED.pos_updated_at,
      void_state          = EXCLUDED.void_state,
      void_status         = EXCLUDED.void_status,
      surcharge_amount    = EXCLUDED.surcharge_amount,
      surcharge_rate      = EXCLUDED.surcharge_rate,
      amount_redeemed     = EXCLUDED.amount_redeemed,
      change_amount       = EXCLUDED.change_amount,
      created_at          = now()
    WHERE EXCLUDED.pos_updated_at >= bailey_payments.pos_updated_at
    `,
    [
      tx.id, o.id, siteId, tx.posTransactionId,
      tx.paymentTotal != null ? Math.max(0, Number(tx.paymentTotal) / 100) : 0,
      tx.tips != null ? Math.max(0, Number(tx.tips) / 100) : 0,
      tx.paymentState, tx.transactionState, tx.method?.name, tx.detail?.cardType,
      tx.detail?.pan, tx.posCreatedAt, tx.posUpdatedAt,
      tx.index,
      tx.amountRedeemed != null ? Math.max(0, Number(tx.amountRedeemed) / 100) : 0,
      tx.changeAmount != null ? Math.max(0, Number(tx.changeAmount) / 100) : 0,
      tx.memberID ?? null,
      tx.surchargeAmount != null ? Math.max(0, Number(tx.surchargeAmount) / 100) : 0,
      tx.surchargeRate, tx.voidState, tx.voidStatus,
      tx.linkedTransactionId ?? null, tx.linkedTransaction ?? null,
      tx.method?.id ?? null, tx.method?.identifier ?? null, tx.method?.type ?? null, tx.method?.typeIdentifier ?? null,
      tx.detail?.id ?? null, tx.detail?.merchantId ?? null, tx.detail?.terminalId ?? null, tx.detail?.referenceId ?? null,
      tx.detail?.stan ?? null, tx.detail?.rrn ?? null, tx.detail?.authCode ?? null,
      tx.detail?.systemTransactionDate ?? null, tx.detail?.currency ?? null, tx.detail?.transactionType ?? null,
      tx.source?.applicationId ?? null, tx.source?.name ?? null, tx.source?.externalUUID ?? null,
      tx.version ?? null, tx.actionLog?.id ?? null, tx.actionLog?.performedAt ?? null,
      tx.actionLog?.station?.id ?? null, tx.actionLog?.station?.identifier ?? null, tx.actionLog?.station?.stationName ?? null, tx.actionLog?.station?.locationName ?? null,
      tx.actionLog?.employee?.identifier ?? null, tx.actionLog?.employee?.displayName ?? null, tx.actionLog?.authorisedBy ?? null
    ]
  );
}


}

// ---------------------------------------------------------------------
// 5. aura_transactions_raw payload -- only for paid orders, non-cash
//    Purchase transactions
// ---------------------------------------------------------------------
function isCash(tx) {
  const mode = tx.method?.name ?? tx.method?.type ?? "";
  return mode.toLowerCase() === "cash";
}




exports.handler = async (event) => {
  const LOOKBACK_MINUTES = Number(process.env.LOOKBACK_MINUTES || 30);
  const now = new Date();
  const defaultUpdatedTo = now.toISOString().replace('T', ' ').replace('Z', '');

  const token = await getAuthToken();
  const allSites = await getSitesByManagementId(
    process.env.GROUP_ID || 26,
    token
  );

  let totalOrdersIngested = 0;
  let totalAuraInserts = 0;
  let totalFailedOrders = 0;

  // Single DB connection for entire Lambda
  const client = await pool.connect();
  try {
    for (const site of allSites) 
    {
      const { siteId, siteName } = site;

      let auraInserts = 0;
      let failedOrders = 0;

      if (!SITES.includes(Number(siteId))) {
        continue; // skip this site, keep processing the rest
      }

      try {
        const lastSynced = await getLastCheckpoint(client, siteId);

        const params = {};

        if (lastSynced) {
          params.UpdatedFrom = lastSynced
          params.UpdatedTo = defaultUpdatedTo;
        }

        const orders = await fetchOrders(          
          siteId,
          params,
          token
        );
        
        console.log(
          `Fetched ${orders.length} orders for site ${siteId}, ${params.UpdatedFrom} -> ${params.UpdatedTo}`
        );

        for (const o of orders) {
          try {
            await client.query("BEGIN");
               //==========ME&U
             if(EXCLUDE_MEU_TRX)
              {
               const meu = await isMeuTrx(client, o); 
               if (meu) {
                 await client.query("COMMIT");
                 continue;
               }
              }
             
              //==========END

            await ingestOrder(client,o,siteId)
            await client.query("COMMIT");
          } catch (err) {
            failedOrders++;

            console.error(
              `Error processing order ${o.id}:`,
              err.message
            );

              try {
                await client.query("ROLLBACK");
              } catch (rollbackErr) {
                console.error(
                  `Rollback failed for order ${o.id}:`,
                  rollbackErr.message
                );
              }
          }
        }

        console.log(
          `Site ${siteId} complete. Orders=${orders.length}, Aura Inserts=${auraInserts}, Failed=${failedOrders}`
        );

        totalOrdersIngested += orders.length;
        totalAuraInserts += auraInserts;
        totalFailedOrders += failedOrders;
        if (orders.length) 
        {
            const maxUpdatedAt = orders.reduce( (m, o) => new Date(o.updatedAt) > new Date(m) ? o.updatedAt : m, orders[0].updatedAt);

            await saveCheckpoint(client, siteId, maxUpdatedAt);
          }
      } catch (err) {
        console.error(
          `Site ${siteId} failed:`,
          err.message
        );

        // Continue with next site instead of failing whole Lambda
        totalFailedOrders++;
        continue;
      }
    }

    return {
      ordersIngested: totalOrdersIngested,
      auraInserts: totalAuraInserts,
      failedOrders: totalFailedOrders,
    };
  } catch (err) {
    console.error("Lambda failed:", err);
    throw err;
  } finally {
    client.release();
  }
};

function safeDivideBy100(value) {
  if (value == null) return 0;           // null/undefined → 0
  const num = Number(value);
  if (isNaN(num)) return 0;              // non-numeric → 0
  return Math.max(0, num / 100);         // clamp negatives to 0
}

async function fetchAndIngestOrders(client, siteId, paramsObj, token) {
  let page = 1;
  let totalIngested = 0;
  let allData = []
  while (true) {
    const params = { Page: page, Limit: PAGE_LIMIT,...paramsObj };
   
    const res = await axios.get(`${process.env.IMPOS_BASE_URL}/v1/sites/${siteId}/orders`, {
      params,
      headers: { accept: "text/plain", Authorization: `Bearer ${token}` },
    });

    const records = res.data?.records ?? [];
    const meta = res.data?.metadata;
    console.log(`siteId => ${siteId} fetched total : ${records.length} page : ${page}`)
    for (const o of records) {
      await ingestOrder(client, o, siteId); // your existing per-order upsert
      allData.push(o)
      totalIngested++;
    }

    // Checkpoint after every page, not just at the end -- if the Lambda
    // times out mid-backfill, the next invocation resumes from here
    // instead of re-pulling from page 1.
    if (records.length) {
      const maxUpdatedAt = records.reduce((m, o) => (o.updatedAt > m ? o.updatedAt : m), records[0].updatedAt);
      await saveCheckpoint(client, siteId, maxUpdatedAt);
    }

    if (!meta || !meta.hasNext) break;
    page += 1;
  }
  console.log("totalIngested ",totalIngested)
  return allData;
}


// FIX: was missing `async` (used `await` inside a plain arrow fn ->
// SyntaxError that would prevent the whole module from loading), and
// the call site wasn't passing `token`.
const getSitesByManagementId = async (managementGroupId, token) => {
  const res = await axios.get(`${process.env.IMPOS_BASE_URL}/v1/sites`, {
    headers: { accept: "text/plain", Authorization: `Bearer ${token}` },
    // timeout: 20000,
  });

  const records = res.data ?? [];
  return records;
};
const getMemberId = async (firstSix, lastFour) => {

  const response = await axios.post(`https://7ivzcls5ab.execute-api.ap-southeast-2.amazonaws.com/default/card_matching_service`,
    {
      firstSix,
      lastFour
    });
  return response.data?.member_id || null;
};

const getMemberIdByExternalId = async (client, referral_id) => {

  const { rows } = await safeQuery(client,
    `SELECT member_id FROM aura_customer WHERE referral_id = $1`,
    [referral_id]
  ); 
  return rows[0]?.member_id || null;
};


/**
 * Scans transactions in order and returns the first resolvable memberId,
 * checking memberID then card for each transaction before moving to the next.
 */

function isApprovedForValue(o) {
  return (
    o.voidState !== "voided" &&
    o.paymentState !== "unpaid" &&
    o.voidState !== "refunded" &&
    o.orderState !== "voided"
  );
}

async function resolveMemberIdForOrder(client, order) {
  const orderId = order.id;
  const cardNumber = order.member?.cardNumber
    ? String(order.member.cardNumber).trim()
    : null;

  // --- Priority 1: order.member.cardNumber -> referral_id lookup ---
  // Covers partial redemption, full redemption, and Aura barcode scan --
  // all three just mean cardNumber is populated, so no branching needed.
  if (cardNumber) {
    const memberId = await getMemberIdByExternalId(client, cardNumber);
    if (memberId) return memberId;

    console.warn(
      `Order ${orderId}: order.member.cardNumber "${cardNumber}" present but no match via getMemberIdByExternalId`
    );
  }

  // --- Priority 2: order.member.cardNumber empty -> fall back to transaction card ---
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
    if (memberId) return memberId; // "matching credit card"
    // else: "non-matching credit card" -> just continue to next tx, no member found
  }

  return null;
}


async function getLastCheckpoint(client, siteId) {
  const { rows } = await safeQuery(client,
    `SELECT to_char(last_synced_updated_at - INTERVAL '30 seconds','YYYY-MM-DD HH24:MI:SS') as last_synced_updated_at
     FROM impos_sync_checkpoints WHERE site_id = $1 and group_id = $2`,
    [siteId, process.env.GROUP_ID || 26]
  );
  return rows[0]?.last_synced_updated_at ?? null;
}

async function saveCheckpoint(client, siteId, maxUpdatedAt) {
  await safeQuery(client,
    `INSERT INTO impos_sync_checkpoints (site_id,group_id, last_synced_updated_at, updated_at)
     VALUES ($1, $2, $3, now())
     ON CONFLICT (site_id, group_id) DO UPDATE SET
       last_synced_updated_at = EXCLUDED.last_synced_updated_at,
       updated_at = now()
     WHERE EXCLUDED.last_synced_updated_at > impos_sync_checkpoints.last_synced_updated_at`,
    [siteId, process.env.GROUP_ID || 12, maxUpdatedAt]
  );
}

async function isMeuTrx(client, payload) {

  try {
    
  const externalUUID = payload?.source?.externalUUID;

  if (!externalUUID) {
    console.warn("externalUUID not found in payload");
    return false;
  }

  // Not a me&u transaction => allow ingest
  if (!externalUUID.startsWith("me&u")) {
    console.log(`Not a me&u transaction: ${externalUUID}`);
    return false;
  }

  // Store excluded transaction
  await client.query(
    `
      INSERT INTO impos_meu_excluded_trx (payload)
      VALUES ($1::jsonb)
    `,
    [JSON.stringify(payload)]
  );

  console.log(`Excluded me&u transaction logged: ${externalUUID}`);

  // Prevent further ingestion
  return true;
  
  } catch (e) {
    console.error(e)
    return true;
  }
}

// (async()=>{
// await this.handler({})
// })()



