
// require("dotenv").config();
// const PARTNER_NAME ='The Milton Rooftop & Bar'
// const PARTNER_ID='IMPOS-202607'


// const pg = require("pg");
// const { Pool } = pg;
// const pool = new Pool({
//   host: process.env.HOST,
//   database: process.env.DBNAME,
//   user: process.env.DB_USER,
//   password: process.env.DB_PASSWORD,
//   port: process.env.PORT,
//   max: 3,
// });



let _exclusionRulesCache = new Map(); // keyed by partner_id, since rules are partner-scoped
let _exclusionRulesCachedAt = new Map();
const EXCLUSION_RULES_TTL_MS = 60 * 60 * 1000;

async function loadPaymentExclusionRules(client, partnerId, { forceRefresh = false } = {}) {
  try {
    const cachedAt = _exclusionRulesCachedAt.get(partnerId) || 0;
    const isStale = Date.now() - cachedAt > EXCLUSION_RULES_TTL_MS;

    if (!forceRefresh && _exclusionRulesCache.has(partnerId) && !isStale) {
      console.log(`CACHE HIT for partner ${partnerId}:`, _exclusionRulesCache.get(partnerId));
      return _exclusionRulesCache.get(partnerId);
    }

    const { rows } = await safeQuery(client,
      `SELECT flat_column_name, rule_value, rule_key
       FROM payment_exclusion_rules
       WHERE active = true
         AND partner_id = $1`,
      [partnerId]
    );

    const rules = rows.map((r) => ({
      column: r.flat_column_name,
      value: String(r.rule_value).trim().toLowerCase(),
      sourcePath: r.rule_key,
    }));

    _exclusionRulesCache.set(partnerId, rules);
    _exclusionRulesCachedAt.set(partnerId, Date.now());
    console.log("DB RULES ", rules);
    return rules;
  } catch (err) {
    console.error(`loadPaymentExclusionRules failed for partner ${partnerId}:`, err.message);
    // Fail safe: no rules = nothing excluded, caller proceeds with full checkTotal.
    // Do NOT cache this empty result — next call should retry the DB, not get stuck.
    return [];
  }
}

function paymentMatchesExclusionRule(paymentRow, rules) {
  try {
    for (const rule of rules) {
      const actual = paymentRow?.[rule.column];
      if (actual === undefined || actual === null) continue;
      if (String(actual).trim().toLowerCase() === rule.value) {
        return rule;
      }
    }
    return null;
  } catch (err) {
    console.error("paymentMatchesExclusionRule failed:", err.message, "row:", paymentRow);
    return null; // treat as "no match" rather than crash the whole batch
  }
}

function calculatePaymentExclusionAmount(paymentRows, rules, amountField = 'payment_total') {
  let excludedAmount = 0;
  const excluded = [];

  try {
    for (const p of paymentRows || []) {
      const matchedRule = paymentMatchesExclusionRule(p, rules);
      if (matchedRule) {
        const amt = Number(p[amountField]) || 0;
        excludedAmount += amt;
        excluded.push({ payment: p, matchedRule, amount: amt });
      }
    }
  } catch (err) {
    console.error("calculatePaymentExclusionAmount failed:", err.message);
    return { excludedAmount: 0, excluded: [] }; // safe default, don't propagate partial sums
  }

  return { excludedAmount, excluded };
}

exports.applyPaymentExclusion = async (client, payments, partnerId) => {
  try {
    const rules = await loadPaymentExclusionRules(client, partnerId);
    const { excludedAmount, excluded } = calculatePaymentExclusionAmount(payments, rules, 'payment_total');

    if (excluded.length > 0) {
      excluded.forEach(({ payment, matchedRule, amount }) => {
        console.log(
          `Excluding $${amount} (payment ${payment.id}, ` +
          `${matchedRule.column}=${payment[matchedRule.column]}, rule="${matchedRule.sourcePath}")`
        );
      });
    }

    return excludedAmount;
  } catch (err) {
    // Last-resort catch-all. Should rarely trigger since the two functions
    // above already catch internally, but this guarantees the exported
    // function itself NEVER throws to the caller, regardless of what changes
    // inside it later.
    console.error(`applyPaymentExclusion failed for partner ${partnerId}:`, err.message);
    return 0;
  }
};
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

// (async()=>{
// const client = await pool.connect();

// // --- dummy payments array, shaped like froth_payments rows ---
// const DUMMY_PAYMENTS = [
//   {
//     id: 1,
//     payment_amount: 25.00,
//     method_type: 'PC-EFTPOS',
//     method_type_indentifier: "66",
//     pan_masked: '433273XXXXXX1234',
//   },
//   {
//     id: 2,
//     payment_amount: 15.50,
//     method_type: 'ImposCloudLoyalty',        // should match rule 1
//     method_type_indentifier: "136",             // also matches rule 2
//     pan_masked: null,
//   } ,
//   {
//     id: 3,
//     payment_amount: 2,
//     method_type: 'ImposCloudLoyalty',        // should match rule 1
//     method_type_indentifier: "136",             // also matches rule 2
//     pan_masked: null,
//   } 
// ];

// const checkTotal=100
// const excludeAmount = await this.applyPaymentExclusion(client, DUMMY_PAYMENTS, PARTNER_ID);
// const trx_amount_modified = Math.max(0, checkTotal - excludeAmount);
// console.log(`Check total: ${checkTotal}, Excluded: ${excludeAmount}, Modified total: ${trx_amount_modified}`);

// client.release();
// })()





