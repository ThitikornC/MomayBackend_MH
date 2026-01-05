// server.js
require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors({ origin: '*' })); // สำหรับ dev เท่านั้น

// ================= MongoDB =================
const mongoUri = process.env.MONGODB_URI;
if (!mongoUri) {
    console.error('❌ MONGODB_URI not set in .env');
    process.exit(1);
}

mongoose.connect(mongoUri)
  .then(() => console.log('✅ Connected to MongoDB Atlas'))
  .catch(err => {
    console.error('❌ MongoDB connection error:', err);
    process.exit(1);
  });

// ================= Schema =================
const px_pm3250_schema = new mongoose.Schema({
  voltage: Number,
  current: Number,
  power: Number,
  active_power_phase_a: Number,
  active_power_phase_b: Number,
  active_power_phase_c: Number,
  voltage1: Number,
  voltage2: Number,
  voltage3: Number,
  voltageln: Number,
  voltagell: Number,
  // Additional detailed fields
  current_a: Number,
  current_b: Number,
  current_c: Number,
  current_avg: Number,
  current_unbalance_a: Number,
  current_unbalance_b: Number,
  current_unbalance_c: Number,
  voltage_ab: Number,
  voltage_bc: Number,
  voltage_ca: Number,
  voltage_ll_avg: Number,
  voltage_an: Number,
  voltage_bn: Number,
  voltage_cn: Number,
  voltage_ln_avg: Number,
  voltage_unbalance_ab: Number,
  voltage_unbalance_bc: Number,
  voltage_unbalance_ca: Number,
  voltage_unbalance_ll_worst: Number,
  voltage_unbalance_an: Number,
  voltage_unbalance_bn: Number,
  voltage_unbalance_cn: Number,
  voltage_unbalance_ln_worst: Number,
  active_power_a: Number,
  active_power_b: Number,
  active_power_c: Number,
  active_power_total: Number,
  reactive_power_a: Number,
  reactive_power_b: Number,
  reactive_power_c: Number,
  reactive_power_total: Number,
  apparent_power_a: Number,
  apparent_power_b: Number,
  apparent_power_c: Number,
  apparent_power_total: Number,
  power_factor_a: Number,
  power_factor_b: Number,
  power_factor_c: Number,
  power_factor_total: Number,
  frequency: Number,
  mac_address: String,
  // raw payload from receivers (keep raw for debugging + any additional fields)
  raw: mongoose.Schema.Types.Mixed,
  // Store timestamp by default as Thailand local time (UTC+7) to keep
  // compatibility with existing receivers and minimal changes.
  timestamp: { type: Date, default: () => new Date(Date.now() + 7*60*60*1000) }
}, { timestamps: true, strict: false });

// ================= ESP PM models (use same electrical schema)
// PM receivers will store the same detailed electrical measurements
// Use a single collection `pm_doc` for all PM devices
const PM_doc = mongoose.model('pm_doc', px_pm3250_schema);

// Helper: create document from incoming payload using allowed fields and merge extras
async function saveESPDoc(Model, payload) {
  if (!payload || Object.keys(payload).length === 0) throw new Error('Empty payload');

  const allowedFields = [
    'voltage','current','power',
    'active_power_phase_a','active_power_phase_b','active_power_phase_c',
    'voltage1','voltage2','voltage3','voltageln','voltagell',
    'current_a','current_b','current_c','current_avg','current_unbalance_a','current_unbalance_b','current_unbalance_c',
    'voltage_ab','voltage_bc','voltage_ca','voltage_ll_avg','voltage_an','voltage_bn','voltage_cn','voltage_ln_avg',
    'voltage_unbalance_ab','voltage_unbalance_bc','voltage_unbalance_ca','voltage_unbalance_ll_worst',
    'voltage_unbalance_an','voltage_unbalance_bn','voltage_unbalance_cn','voltage_unbalance_ln_worst',
    'active_power_a','active_power_b','active_power_c','active_power_total',
    'reactive_power_a','reactive_power_b','reactive_power_c','reactive_power_total',
    'apparent_power_a','apparent_power_b','apparent_power_c','apparent_power_total',
    'power_factor_a','power_factor_b','power_factor_c','power_factor_total',
    'frequency','mac_address'
  ];

  const docBody = {};
  for (const k of allowedFields) {
    docBody[k] = payload[k] !== undefined ? payload[k] : null;
  }
  for (const k of Object.keys(payload)) {
    if (payload[k] !== undefined) docBody[k] = payload[k];
  }
  if (payload.timestamp) docBody.timestamp = new Date(payload.timestamp);
  if (docBody.raw !== undefined) delete docBody.raw;

  const doc = await Model.create(docBody);
  return doc;
}

// ================= Notification Schemas (แยก collection) =================

// 1. Peak Notifications
const peakNotificationSchema = new mongoose.Schema({
    title: { type: String, required: true },
    body: { type: String, required: true },
    power: { type: Number, required: true },
    timestamp: { type: Date, default: () => new Date(Date.now() + 7*60*60*1000) },
    read: { type: Boolean, default: false }
}, { timestamps: true });

const PeakNotificationH = mongoose.model("peak_notifications_h", peakNotificationSchema);

// 2. Daily Diff Notifications
const dailyDiffNotificationSchema = new mongoose.Schema({
    title: { type: String, required: true },
    body: { type: String, required: true },
    yesterday: {
        date: String,
        energy_kwh: Number,
        electricity_bill: Number,
        samples: Number
    },
    dayBefore: {
        date: String,
        energy_kwh: Number,
        electricity_bill: Number,
        samples: Number
    },
    diff: {
        kWh: Number,
        electricity_bill: Number
    },
    timestamp: { type: Date, default: () => new Date(Date.now() + 7*60*60*1000) },
    read: { type: Boolean, default: false }
}, { timestamps: true });

const DailyDiffNotificationH = mongoose.model("daily_diff_notifications_h", dailyDiffNotificationSchema);

// 3. Test Notifications
const testNotificationSchema = new mongoose.Schema({
    title: { type: String, required: true },
    body: { type: String, required: true },
    timestamp: { type: Date, default: () => new Date(Date.now() + 7*60*60*1000) },
    read: { type: Boolean, default: false }
}, { timestamps: true });

const TestNotificationH = mongoose.model("test_notifications_h", testNotificationSchema);

const dailyBillNotificationSchema = new mongoose.Schema({
    title: { type: String, required: true },
    body: { type: String, required: true },
    date: { type: String, required: true }, // YYYY-MM-DD
    energy_kwh: { type: Number, required: true },
    electricity_bill: { type: Number, required: true },
    samples: { type: Number, default: 0 },
    rate_per_kwh: { type: Number, default: 4.4 },
    timestamp: { type: Date, default: () => new Date(Date.now() + 7*60*60*1000) },
    read: { type: Boolean, default: false }
}, { timestamps: true });

const DailyBillNotificationH = mongoose.model("daily_bill_notifications_h", dailyBillNotificationSchema);

// ================= Helper Functions =================
function calculateBill(energyKwh, ratePerKwh = 4.4) {
    return Number((energyKwh * ratePerKwh).toFixed(2));
}

function getDayRangeUTC(dateStr) {
    const start = new Date(`${dateStr}T00:00:00Z`);
    const end = new Date(`${dateStr}T23:59:59Z`);
    return { start, end };
}
function getDayRangeUTCFromThailand(dateStr) {
    const startTH = new Date(`${dateStr}T00:00:00`);
    const endTH = new Date(`${dateStr}T23:59:59`);
    return { start: new Date(startTH.getTime() - 7*3600*1000),
             end: new Date(endTH.getTime() - 7*3600*1000) };
}
function getMonthRange(yearMonth) {
    const start = new Date(`${yearMonth}-01T00:00:00Z`);
    const nextMonth = new Date(start);
    nextMonth.setMonth(nextMonth.getMonth() + 1);
    return { start, end: nextMonth };
}

// Prefer active_power_total for calculations. Fall back to phase sums or `power`.
function docPower(d) {
  if (!d) return 0;
  // Use only active_power_total for all calculations. If missing, return 0.
  if (d.active_power_total !== undefined && d.active_power_total !== null) return d.active_power_total;
  return 0;
}

// ================= Routes =================

// Health check
app.get('/', (req, res) => {
    res.json({
        status: 'OK',
        service: 'px_pm3250 Daily Bill API',
        version: '1.1.0',
        timestamp: new Date().toISOString()
    });
});

// ================= ESP Receivers =================
// รับข้อมูลจาก ESP ชื่อ pm_doc
app.post('/esp/pm_doc', async (req, res) => {
  try {
    const payload = req.body || {};
    const doc = await saveESPDoc(PM_doc, payload);
    console.log('💾 pm_doc saved:', doc._id);
    res.status(201).json({ success: true, id: doc._id });
  } catch (err) {
    console.error('❌ /esp/pm_doc error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Allow PUT as an alternative to POST for devices that use HTTP PUT
app.put('/esp/pm_doc', async (req, res) => {
  try {
    const payload = req.body || {};
    const doc = await saveESPDoc(PM_doc, payload);
    console.log('💾 pm_doc saved (PUT):', doc._id);
    res.status(201).json({ success: true, id: doc._id });
  } catch (err) {
    console.error('❌ PUT /esp/pm_doc error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Recent fetch endpoint for quick testing (pm_doc)
app.get('/esp/pm_doc/recent', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const docs = await PM_doc.find().sort({ timestamp: -1 }).limit(limit).lean();
    const data = docs.map(d => ({
      timestamp: d.timestamp ? new Date(d.timestamp).toISOString() : null,
      mac_address: d.mac_address || null,
      voltage: d.voltage || null,
      current: d.current || null,
      active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : null
    }));
    res.json(data);
  } catch (err) {
    console.error('❌ GET /esp/pm_doc/recent error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /daily-energy
// Returns active_power_total and timestamp from pm_doc for a given date
app.get('/daily-energy', async (req, res) => {
  try {
    const queryDate = req.query.date || new Date().toISOString().slice(0,10); // YYYY-MM-DD
    if (!/^\d{4}-\d{2}-\d{2}$/.test(queryDate)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const start = new Date(`${queryDate}T00:00:00Z`);
    const end = new Date(`${queryDate}T23:59:59Z`);
    const limit = parseInt(req.query.limit) || 10000;

    // Fetch pm_doc data for the given date range
    const docsRaw = await PM_doc.find({ timestamp: { $gte: start, $lte: end } })
      .sort({ timestamp: 1 })
      .limit(limit)
      .lean();

    // normalize to series of { timestamp, value }
    const toValue = (doc) => {
      if (!doc) return null;
      const v = docPower(doc);
      return v === 0 ? null : v;
    };

    const points = docsRaw.map(d => ({ timestamp: d.timestamp, value: toValue(d) }));

    res.json({
      date: queryDate,
      series: [
        { label: 'pm_doc', points }
      ]
    });
  } catch (err) {
    console.error('❌ GET /daily-energy error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Legacy-format endpoint: GET /daily-energy/:source
// Returns { message, data: [...] } where each data item contains the schema fields
app.get('/daily-energy/:source', async (req, res) => {
  try {
    const source = (req.params.source || '').toLowerCase(); // e.g. pm_doc
    const queryDate = req.query.date || new Date().toISOString().slice(0,10);
    if (!/^[a-z0-9_\-]+$/.test(source)) return res.status(400).json({ error: 'Invalid source' });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(queryDate)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const start = new Date(`${queryDate}T00:00:00Z`);
    const end = new Date(`${queryDate}T23:59:59Z`);
    const limit = parseInt(req.query.limit) || 10000;

    let Model;
    // Map source to unified pm_doc collection
    if (source === 'pm_doc' || source === 'pm-doc' || source === 'px_pm3250') Model = PM_doc;
    else return res.status(400).json({ error: 'Unknown source. Use pm_doc' });

    const docs = await Model.find({ timestamp: { $gte: start, $lte: end } })
                          .sort({ timestamp: 1 })
                          .limit(limit)
                          .lean();

    // Fields to return (use schema fields)
    const fields = [
      '_id','voltage','current','active_power_total','mac_address','timestamp'
    ];

    const data = docs.map(d => {
      const out = {};
      for (const f of fields) {
        if (d[f] !== undefined) out[f] = d[f];
        else out[f] = null;
      }
      // Present timestamp in Thailand local time (Asia/Bangkok) in ISO-like format
      if (d.timestamp) {
        const dateObj = new Date(d.timestamp);
        const fmt = new Intl.DateTimeFormat('sv-SE', {
          timeZone: 'Asia/Bangkok',
          hour12: false,
          year: 'numeric', month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit', second: '2-digit'
        });
        // fmt produces "YYYY-MM-DD HH:mm:ss" — convert space to 'T' and append milliseconds
        out.timestamp = fmt.format(dateObj).replace(' ', 'T') + '.000';
      } else {
        out.timestamp = null;
      }
      return out;
    });

    res.json({ message: 'Data retrieved successfully', data });
  } catch (err) {
    console.error('❌ GET /daily-energy/:source error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH endpoints to update a specific document by id (safe field filter)
// Unified PATCH endpoint for pm_doc documents
app.patch('/esp/pm_doc/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const allowed = [
      'voltage','current','power','active_power_phase_a','active_power_phase_b','active_power_phase_c',
      'voltage1','voltage2','voltage3','voltageln','voltagell',
      'current_a','current_b','current_c','current_avg','current_unbalance_a','current_unbalance_b','current_unbalance_c',
      'voltage_ab','voltage_bc','voltage_ca','voltage_ll_avg','voltage_an','voltage_bn','voltage_cn','voltage_ln_avg',
      'voltage_unbalance_ab','voltage_unbalance_bc','voltage_unbalance_ca','voltage_unbalance_ll_worst',
      'voltage_unbalance_an','voltage_unbalance_bn','voltage_unbalance_cn','voltage_unbalance_ln_worst',
      'active_power_a','active_power_b','active_power_c','active_power_total',
      'reactive_power_a','reactive_power_b','reactive_power_c','reactive_power_total',
      'apparent_power_a','apparent_power_b','apparent_power_c','apparent_power_total',
      'power_factor_a','power_factor_b','power_factor_c','power_factor_total',
      'frequency','mac_address','timestamp','raw'
    ];
    const updates = {};
    for (const k of Object.keys(req.body || {})) {
      if (allowed.includes(k)) updates[k] = req.body[k];
    }
    if (!Object.keys(updates).length) return res.status(400).json({ error: 'No allowed fields to update' });
    if (updates.timestamp) updates.timestamp = new Date(updates.timestamp);
    const doc = await PM_doc.findByIdAndUpdate(id, updates, { new: true });
    if (!doc) return res.status(404).json({ error: 'Document not found' });
    res.json({ success: true, doc });
  } catch (err) {
    console.error('❌ PATCH /esp/pm_doc/:id error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ================= Daily Bill =================
app.get('/daily-bill', async (req, res) => {
    try {
        const today = new Date().toLocaleDateString('en-CA');
        const selectedDate = req.query.date || today;

        if (!/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
            return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD', example: '2025-09-30' });
        }

        const { start, end } = getDayRangeUTC(selectedDate);

          // Use pm_doc and prefer active_power_total when available
          const data = await PM_doc.find({ timestamp: { $gte: start, $lte: end } })
            .sort({ timestamp: 1 })
            .select('active_power_total timestamp');

        if (!data.length) {
            return res.status(404).json({
                error: `No data found for ${selectedDate}`,
                date: selectedDate,
                total_energy_kwh: 0,
                electricity_bill: 0
            });
        }

        let totalEnergyKwh = 0;
        let maxPower = 0;
        let minPower = Infinity;
        let totalPowerSum = 0;

        // use top-level docPower helper (prefers active_power_total)

        for (let i = 0; i < data.length; i++) {
          const p = docPower(data[i]);
          totalPowerSum += p;
          if (p > maxPower) maxPower = p;
          if (p < minPower) minPower = p;

          if (i === 0) continue;
          const prevP = docPower(data[i-1]);
          const intervalHours = (data[i].timestamp - data[i-1].timestamp) / 1000 / 3600;
          totalEnergyKwh += ((p + prevP) / 2) * intervalHours;
        }

        const avgPower = totalPowerSum / data.length;
        const electricityBill = calculateBill(totalEnergyKwh);

        res.json({
            date: selectedDate,
            samples: data.length,
            total_energy_kwh: Number(totalEnergyKwh.toFixed(2)),
            avg_power_kw: Number(avgPower.toFixed(2)),
            max_power_kw: Number(maxPower.toFixed(2)),
            min_power_kw: Number(minPower.toFixed(2)),
            electricity_bill: electricityBill,
            rate_per_kwh: 4.4
        });
    } catch (err) {
        console.error('❌ /daily-bill error:', err);
        res.status(500).json({ error: 'Failed to process data', message: err.message });
    }
});

app.get('/daily-bill/:date', async (req, res) => {
    req.query.date = req.params.date;
    return app._router.handle(req, res);
});

// ================= Daily Calendar (Optimized) =================
app.get('/calendar', async (req, res) => {
  try {
    const now = new Date();

    // เดือนปัจจุบันและเดือนก่อนหน้า
    const startPrev = new Date(Date.UTC(now.getFullYear(), now.getMonth() - 1, 1));
    const endCurrent = new Date(Date.UTC(now.getFullYear(), now.getMonth() + 1, 1));

    // Aggregation pipeline: group by local Thailand date and compute daily energy (kWh)
    const agg = await PM_doc.aggregate([
      {
        $match: {
          timestamp: { $gte: startPrev, $lt: endCurrent }
        }
      },
      { $sort: { timestamp: 1 } },
      {
        $group: {
          _id: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } },
          powers: { $push: { $ifNull: ["$active_power_total", 0] } },
          timestamps: { $push: "$timestamp" }
        }
      },
      {
        $project: {
          powers: 1,
          timestamps: 1,
          totalEnergyKwh: {
            $let: {
              vars: { arrP: "$powers", arrT: "$timestamps" },
              in: {
                $reduce: {
                  input: { $range: [1, { $size: "$$arrP" }] },
                  initialValue: 0,
                  in: {
                    $add: [
                      "$$value",
                      {
                        $let: {
                          vars: {
                            pCurr: { $arrayElemAt: ["$$arrP", "$$this"] },
                            pPrev: { $arrayElemAt: ["$$arrP", { $subtract: ["$$this", 1] }] },
                            tCurr: { $arrayElemAt: ["$$arrT", "$$this"] },
                            tPrev: { $arrayElemAt: ["$$arrT", { $subtract: ["$$this", 1] }] }
                          },
                          in: {
                            $multiply: [
                              { $divide: [ { $add: ["$$pCurr", "$$pPrev"] }, 2 ] },
                              { $divide: [ { $subtract: ["$$tCurr", "$$tPrev"] }, 3600000 ] }
                            ]
                          }
                        }
                      }
                    ]
                  }
                }
              }
            }
          }
        }
      },
      { $sort: { "_id": 1 } }
    ]);

    // สร้าง events ตาม format เดิม
    const events = agg.flatMap(day => {
      const totalEnergyKwh = Number(day.totalEnergyKwh.toFixed(2));
      const bill = calculateBill(totalEnergyKwh);

      return [
        {
          title: `${totalEnergyKwh} Unit`,
          start: day._id,
          extendedProps: { type: "energy", display_text: `${totalEnergyKwh} Unit` }
        },
        {
          title: `${bill}฿`,
          start: day._id,
          extendedProps: { type: "bill", display_text: `${bill}฿` }
        }
      ];
    });

    res.json(events);

  } catch (err) {
    console.error("❌ /calendar error:", err);
    res.status(500).json({ error: "Failed to get calendar data", message: err.message });
  }
});


// ================= Daily Diff =================
app.get('/daily-diff', async (req, res) => {
    try {
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1);
        const dayBefore = new Date(today);
        dayBefore.setDate(today.getDate() - 2);

        const formatDate = (date) => date.toLocaleDateString('en-CA');

        const getDailyEnergy = async (dateStr) => {
          const { start, end } = getDayRangeUTC(dateStr);
          const dayData = await PM_doc.find({ timestamp: { $gte: start, $lte: end } })
                           .sort({ timestamp: 1 })
                          .select('active_power_total timestamp');

          if (!dayData.length) return { energy_kwh: 0, samples: 0, electricity_bill: 0 };

          let totalEnergyKwh = 0;
          let count = 0;
          let totalPower = 0;
          for (let i = 0; i < dayData.length; i++) {
            const p = docPower(dayData[i]);
            totalPower += p;
            count++;
            if (i === 0) continue;
            const prevP = docPower(dayData[i-1]);
            const intervalHours = (dayData[i].timestamp - dayData[i-1].timestamp) / 1000 / 3600;
            totalEnergyKwh += ((p + prevP) / 2) * intervalHours;
          }

          return { energy_kwh: Number(totalEnergyKwh.toFixed(2)), samples: count, electricity_bill: calculateBill(totalEnergyKwh) };
        };

        const yestData = await getDailyEnergy(formatDate(yesterday));
        const dayBeforeData = await getDailyEnergy(formatDate(dayBefore));

        const diffKwh = Number((dayBeforeData.energy_kwh - yestData.energy_kwh ).toFixed(2));
        const diffBill = Number((dayBeforeData.electricity_bill - yestData.electricity_bill).toFixed(2));

        res.json({
            yesterday: { date: formatDate(yesterday), ...yestData },
            dayBefore: { date: formatDate(dayBefore), ...dayBeforeData },
            diff: { kWh: diffKwh, electricity_bill: diffBill }
        });

    } catch (err) {
        console.error('❌ /daily-diff error:', err);
        res.status(500).json({ error: 'Failed to get daily diff', message: err.message });
    }
});

function addEnergyToHours(prev, curr, hourlyEnergy) {
    let start = new Date(prev.timestamp);
    const end = new Date(curr.timestamp);
  const power = (docPower(prev) + docPower(curr)) / 2;

    while (start < end) {
        const nextHour = new Date(start);
        nextHour.setMinutes(60, 0, 0);
        const intervalEnd = nextHour < end ? nextHour : end;
        const intervalHours = (intervalEnd - start) / 1000 / 3600;

        const hourKey = start.getHours();
        if (!hourlyEnergy[hourKey]) hourlyEnergy[hourKey] = 0;
        hourlyEnergy[hourKey] += power * intervalHours;

        start = intervalEnd;
    }
}

// ================= Hourly Bill =================
app.get('/hourly-bill/:date', async (req, res) => {
    try {
        const selectedDate = req.params.date;
        if (!/^\d{4}-\d{2}-\d{2}$/.test(selectedDate)) {
            return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
        }

        const start = new Date(`${selectedDate}T00:00:00`);
        const end = new Date(`${selectedDate}T23:59:59`);

        const data = await PM_doc.find({ timestamp: { $gte: start, $lte: end } })
              .sort({ timestamp: 1 })
              .select('active_power_total timestamp');

        const hourlyEnergy = Array.from({length:24}, ()=>0);

        if (data.length === 0) {
            return res.json({
                date: selectedDate,
                hourly: hourlyEnergy.map((e,h)=>({
                    hour: `${h.toString().padStart(2,'0')}:00`,
                    energy_kwh: 0,
                    electricity_bill: 0
                }))
            });
        }

            function addEnergy(prev, curr) {
            let startTime = new Date(prev.timestamp);
            const endTime = new Date(curr.timestamp);
            const avgPower = (docPower(prev) + docPower(curr))/2;

            while (startTime < endTime) {
                const nextHour = new Date(startTime);
                nextHour.setMinutes(60,0,0);
                const intervalEnd = nextHour < endTime ? nextHour : endTime;
                const intervalHours = (intervalEnd - startTime)/1000/3600;

                const hour = startTime.getHours();
                hourlyEnergy[hour] += avgPower * intervalHours;

                startTime = intervalEnd;
            }
        }

        for (let i = 1; i < data.length; i++) {
            addEnergy(data[i-1], data[i]);
        }

        const now = new Date();
        if (selectedDate === now.toISOString().slice(0,10)) {
            for (let h = now.getHours()+1; h < 24; h++) {
                hourlyEnergy[h] = 0;
            }
        }

        const hourlyArray = hourlyEnergy.map((energy, h) => ({
            hour: `${h.toString().padStart(2,'0')}:00`,
            energy_kwh: Number(energy.toFixed(2)),
            electricity_bill: Number((energy*4.4).toFixed(2))
        }));

        res.json({
            date: selectedDate,
            hourly: hourlyArray
        });

    } catch (err) {
        console.error('❌ /hourly-bill error:', err);
        res.status(500).json({ error: 'Failed to get hourly bill', message: err.message });
    }
});

// ================= Minute Power Range =================
app.get('/minute-power-range', async (req, res) => {
    try {
        const { date, startHour, endHour } = req.query;

        if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
            return res.status(400).json({
                error: "Missing or invalid date",
                example: "/minute-power-range?date=2025-10-03&startHour=8&endHour=17"
            });
        }

        let { start, end } = getDayRangeUTC(date);

        if (startHour !== undefined) start.setUTCHours(Number(startHour), 0, 0, 0);
        if (endHour !== undefined) end.setUTCHours(Number(endHour), 59, 59, 999);

        const data = await PM_doc.find({
            timestamp: { $gte: start, $lte: end }
        }).sort({ timestamp: 1 })
          .select('timestamp active_power_total voltage current');

        const result = data.map(d => ({
          timestamp: d.timestamp.toISOString(),
          active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : docPower(d),
          voltage: d.voltage,
          current: d.current
        }));

        res.json(result);

    } catch (err) {
        console.error('❌ /minute-power-range error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ================= Hourly Summary =================
app.get('/hourly-summary', async (req, res) => {
    try {
        const { date } = req.query;

        if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
            return res.status(400).json({
                error: "Missing or invalid date",
                example: "/hourly-summary?date=2025-10-03"
            });
        }

        const { start, end } = getDayRangeUTC(date);

        const data = await PM_doc.find({
          timestamp: { $gte: start, $lte: end }
        }).sort({ timestamp: 1 }).select('timestamp active_power_total');

        const hourly = Array.from({ length: 24 }, (_, i) => ({
            hour: `${i.toString().padStart(2,'0')}:00`,
            energy_kwh: 0,
            electricity_bill: 0
        }));

        for (let i = 1; i < data.length; i++) {
            const prev = data[i-1];
            const curr = data[i];

            const intervalHours = (curr.timestamp - prev.timestamp) / 1000 / 3600;
            const avgPower = (docPower(curr) + docPower(prev)) / 2;
            const energyKwh = avgPower * intervalHours;

            const hourKey = prev.timestamp.getUTCHours();
            hourly[hourKey].energy_kwh += energyKwh;
        }

        hourly.forEach(h => {
            h.energy_kwh = Number(h.energy_kwh.toFixed(2));
            h.electricity_bill = Number((h.energy_kwh * 4.4).toFixed(2));
        });

        res.json({
            date,
            hourly
        });

    } catch (err) {
        console.error('❌ /hourly-summary error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ================= Session =================
const session = require('express-session');
const MongoStore = require('connect-mongo');

app.use(session({
    secret: process.env.SESSION_SECRET || 'keyboard_cat',
    resave: false,
    saveUninitialized: false,
    store: MongoStore.create({ mongoUrl: process.env.MONGODB_URI }),
    cookie: { maxAge: 24*60*60*1000 }
}));

// ================= Daily Diff Popup =================
app.get('/daily-diff-popup', async (req, res) => {
    try {
        const todayStr = new Date().toISOString().split('T')[0];

        if (!req.session.lastPopupDate || req.session.lastPopupDate !== todayStr) {
            const axios = require('axios');
            const diffResp = await axios.get(`http://localhost:${PORT}/daily-diff`);

            req.session.lastPopupDate = todayStr;

            return res.json({
                showPopup: true,
                data: diffResp.data
            });
        }

        res.json({ showPopup: false });

    } catch (err) {
        console.error('❌ /daily-diff-popup error:', err.message);
        res.status(500).json({ showPopup: false, error: err.message });
    }
});

// ================= Solar Size =================
app.get('/solar-size', async (req, res) => {
    try {
        const { date, ratePerKwh = 4.4 } = req.query;

        if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
            return res.status(400).json({ 
                error: "Missing or invalid date. Use YYYY-MM-DD",
                example: "/solar-size?date=2025-10-07"
            });
        }

        const start = new Date(`${date}T00:00:00Z`);
        const end = new Date(`${date}T23:59:59Z`);

        const data = await PM_doc.find({
          timestamp: { $gte: start, $lte: end }
        }).sort({ timestamp: 1 }).select('timestamp active_power_total');

        if (!data.length) {
            return res.status(404).json({
                error: `No data for ${date}`,
                date,
                hourly: Array.from({length:24}, (_,h) => ({
                    hour: `${h.toString().padStart(2,'0')}:00`,
                    energy_kwh: 0,
                    electricity_bill: 0,
                    peak_power: 0
                })),
                dayEnergy: 0,
                nightEnergy: 0,
                totalEnergyKwh: 0,
                solarCapacity_kW: 0,
                peakPowerDay: 0,
                savingsDay: 0,
                savingsMonth: 0,
                savingsYear: 0
            });
        }

        const hourlyEnergy = Array.from({length:24}, () => 0);
        const hourlyPeak = Array.from({length:24}, () => 0);

        for (let i = 1; i < data.length; i++) {
            const prev = data[i-1];
            const curr = data[i];
            let t = new Date(prev.timestamp);
            const endTime = new Date(curr.timestamp);
            const avgPower = (docPower(prev) + docPower(curr)) / 2;

            while (t < endTime) {
                const hourIndex = t.getUTCHours();
                const nextHour = new Date(t);
                nextHour.setUTCHours(nextHour.getUTCHours()+1,0,0,0);
                const intervalEnd = nextHour < endTime ? nextHour : endTime;
                const intervalHours = (intervalEnd - t) / 1000 / 3600;

                hourlyEnergy[hourIndex] += avgPower * intervalHours;
                hourlyPeak[hourIndex] = Math.max(hourlyPeak[hourIndex], docPower(prev), docPower(curr));

                t = intervalEnd;
            }
        }

        const hourlyArray = hourlyEnergy.map((energy,h) => ({
            hour: `${h.toString().padStart(2,'0')}:00`,
            energy_kwh: Number(energy.toFixed(2)),
            electricity_bill: Number((energy*ratePerKwh).toFixed(2)),
            peak_power: Number(hourlyPeak[h].toFixed(2))
        }));

        const dayEnergy = hourlyArray
            .slice(6, 19)
            .reduce((sum,o) => sum + o.energy_kwh, 0);

        const nightEnergy = hourlyArray
            .filter((_,h) => h < 6 || h > 18)
            .reduce((sum,o) => sum + o.energy_kwh, 0);

        const totalEnergyKwh = dayEnergy + nightEnergy;
        const peakPowerDay = Math.max(...hourlyPeak);

        const H_sun = 4;
        const solarCapacity_kW = dayEnergy / H_sun;
        const savingsDay = dayEnergy * ratePerKwh;

        res.json({
            date,
            hourly: hourlyArray,
            dayEnergy: Number(dayEnergy.toFixed(2)),
            nightEnergy: Number(nightEnergy.toFixed(2)),
            dayCost: Number((dayEnergy * ratePerKwh).toFixed(2)),
            nightCost: Number((nightEnergy * ratePerKwh).toFixed(2)),
            totalEnergyKwh: Number(totalEnergyKwh.toFixed(2)),
            totalCost: Number((totalEnergyKwh * ratePerKwh).toFixed(2)),
            sunHours: H_sun,
            solarCapacity_kW: Number(solarCapacity_kW.toFixed(2)),
            peakPowerDay: Number(peakPowerDay.toFixed(2)),
            savingsDay: Number(savingsDay.toFixed(2)),
            savingsMonth: Number((savingsDay*30).toFixed(2)),
            savingsYear: Number((savingsDay*365).toFixed(2))
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: err.message });
    }
});

// ================= Raw Local =================
app.get('/raw-local', async (req, res) => {
  try {
    const { date } = req.query;
    if (!date) return res.status(400).json({ error: 'Missing date' });

    const start = new Date(`${date}T08:00:00+07:00`);
    const end   = new Date(`${date}T09:00:00+07:00`);

    const data = await PM_doc.find({
      timestamp: { $gte: start, $lte: end }
    }).sort({ timestamp: 1 });

    const totalPower = data.reduce((sum, d) => sum + docPower(d), 0);

    const outData = data.map(d => ({
      timestamp: d.timestamp ? new Date(d.timestamp).toISOString() : null,
      mac_address: d.mac_address || null,
      voltage: d.voltage || null,
      current: d.current || null,
      active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : null
    }));

    res.json({
      date,
      period: "08:00-09:00",
      count: outData.length,
      totalPower: Number(totalPower.toFixed(3)),
      data: outData
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/raw-08-09', async (req, res) => {
  try {
    const { date } = req.query;
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: "Invalid date. Use YYYY-MM-DD" });
    }

    const start = new Date(`${date}T08:00:00.000Z`);
    const end = new Date(`${date}T08:59:59.999Z`);

    const data = await PM_doc.find({
      timestamp: { $gte: start, $lte: end }
    }).sort({ timestamp: 1 });

    const totalPower = data.reduce((sum, d) => sum + docPower(d), 0);

    const outData = data.map(d => ({
      timestamp: d.timestamp ? new Date(d.timestamp).toISOString() : null,
      mac_address: d.mac_address || null,
      voltage: d.voltage || null,
      current: d.current || null,
      active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : null
    }));

    res.json({
      date,
      period: "08:00-09:00 UTC",
      count: outData.length,
      totalPower: Number(totalPower.toFixed(3)),
      data: outData
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ================= Diagnostics Range =================
app.get('/diagnostics-range', async (req, res) => {
  try {
    const { start, end } = req.query;

    if (!start || !end) {
      return res.status(400).json({
        error: "Missing query params",
        example: "/diagnostics-range?start=2025-10-02T17:00:00Z&end=2025-10-02T17:05:00Z"
      });
    }

    const data = await PM_doc.find({
      timestamp: {
        $gte: new Date(start),
        $lte: new Date(end)
      }
    })
    .sort({ timestamp: 1 })
    .select('timestamp active_power_total voltage current');

    const result = data.map(d => ({
      _id: d._id,
      voltage: d.voltage,
      current: d.current,
      active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : docPower(d),
      timestamp: d.timestamp.toISOString().replace('Z','')
    }));

    res.json(result);

  } catch (err) {
    console.error('❌ /diagnostics-range error:', err);
    res.status(500).json({ error: "Failed", message: err.message });
  }
});

// ================== PUSH NOTIFICATION SYSTEM ==================
const webpush = require('web-push');
const cron = require('node-cron');

webpush.setVapidDetails(
  'mailto:admin@yourdomain.com',
  'BB2fZ3NOzkWDKOi8H5jhbwICDTv760wIB6ZD2PwmXcUA_B5QXkXtely4b4JZ5v5b88VX1jKa7kRfr94nxqiksqY',
  'jURJII6DrBN9N_8WtNayWs4bXWDNzeb_RyjXnTxaDmo'
);

let pushSubscriptions = [];

// สมัครรับการแจ้งเตือน
app.post('/api/subscribe', (req, res) => {
  const sub = req.body;
  if (!sub || !sub.endpoint) {
    return res.status(400).json({ error: 'Invalid subscription' });
  }

  const exists = pushSubscriptions.find(s => s.endpoint === sub.endpoint);
  if (!exists) pushSubscriptions.push(sub);

  console.log(`✅ Push subscription added (${pushSubscriptions.length} total)`);
  res.status(201).json({ message: 'Subscribed successfully' });
});

// ฟังก์ชันส่ง Push Notification พร้อมบันทึกลง DB แยก collection
async function sendPushNotification(title, body, type = 'test', data = {}) {
  try {
    let notification;

    // Ensure body is a string (for Mongoose schema)
    let bodyStr = body;
    if (typeof body !== 'string') {
      try {
        bodyStr = JSON.stringify(body);
      } catch (e) {
        bodyStr = String(body);
      }
    }

    // 1. บันทึกลง Database แยกตาม type (ใช้ ...LIB)
    // ประกาศ model ...LIB
    const PeakNotificationLIB = mongoose.model("peak_notifications_lib", peakNotificationSchema);
    const DailyDiffNotificationLIB = mongoose.model("daily_diff_notifications_lib", dailyDiffNotificationSchema);
    const DailyBillNotificationLIB = mongoose.model("daily_bill_notifications_lib", dailyBillNotificationSchema);
    const TestNotificationLIB = mongoose.model("test_notifications_lib", testNotificationSchema);

    switch(type) {
      case 'peak':
        notification = await PeakNotificationLIB.create({
          title,
          body: bodyStr,
          power: data.power
        });
        console.log('💾 Peak Notification saved (LIB):', notification._id);
        break;

      case 'daily_diff':
        notification = await DailyDiffNotificationLIB.create({
          title,
          body: bodyStr,
          yesterday: data.yesterday,
          dayBefore: data.dayBefore,
          diff: data.diff
        });
        console.log('💾 Daily Diff Notification saved (LIB):', notification._id);
        break;

      case 'daily_bill':
        notification = await DailyBillNotificationLIB.create({
          title,
          body: bodyStr,
          date: data.date,
          energy_kwh: data.energy_kwh,
          electricity_bill: data.electricity_bill,
          samples: data.samples || 0,
          rate_per_kwh: data.rate_per_kwh || 4.4
        });
        console.log('💾 Daily Bill Notification saved (LIB):', notification._id);
        break;

      case 'test':
        notification = await TestNotificationLIB.create({
          title,
          body: bodyStr
        });
        console.log('💾 Test Notification saved (LIB):', notification._id);
        break;

      default:
        console.error('❌ Unknown notification type:', type);
        return null;
    }

    // 2. ส่ง Push notification
    const payload = JSON.stringify({ title, body, url: '/' });

    if (!pushSubscriptions.length) {
      console.log('⚠️ No push subscriptions to send to');
      return notification;
    }

    for (let i = pushSubscriptions.length - 1; i >= 0; i--) {
      const sub = pushSubscriptions[i];
      try {
        await webpush.sendNotification(sub, payload);
        console.log('📤 Sent notification to', sub.endpoint);
      } catch (err) {
        console.error('❌ Push send error for', sub.endpoint, err.statusCode || err);
        const status = err && err.statusCode;
        if (status === 410 || status === 404) {
          pushSubscriptions.splice(i, 1);
          console.log('🗑 Removed expired subscription', sub.endpoint);
        }
      }
    }

    return notification;
  } catch (err) {
    console.error('❌ Error in sendPushNotification:', err);
    throw err;
  }
}

// ================== REALTIME PEAK CHECK ==================
let dailyPeak = { date: '', maxPower: 0 };

async function checkDailyPeak() {
  try {
    const latest = await PM_doc.findOne().sort({ timestamp: -1 }).select('active_power_total timestamp');
    if (!latest) return;

    const today = new Date().toISOString().split('T')[0];

    if (dailyPeak.date !== today) {
      dailyPeak = { date: today, maxPower: 0 };
      console.log(`🔁 Reset daily peak for ${today}`);
    }

    const powerNow = docPower(latest) || 0;
    if (powerNow > dailyPeak.maxPower) {
      dailyPeak.maxPower = powerNow;
      console.log(`🚨 New peak ${powerNow.toFixed(2)} kW at ${latest.timestamp}`);

      await sendPushNotification(
        '⚡ New Daily Peak!',
      
        { power: powerNow }
      );
    }
  } catch (err) {
    console.error('❌ Error checking daily peak:', err);
  }
}

// ตรวจสอบ peak ทุก 10 วินาที
cron.schedule('*/10 * * * * *', () => {
  checkDailyPeak();
});

// ================== DAILY BILL AUTO NOTIFICATION ==================

async function sendDailyBillNotification() {
  try {
    // คำนวณวันที่เมื่อวาน (เพราะตี 1 = เริ่มวันใหม่แล้ว)
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0]; // YYYY-MM-DD

    console.log(`📊 Calculating daily bill for ${dateStr}...`);

    // ดึงข้อมูลจาก /daily-bill API
    const { start, end } = getDayRangeUTC(dateStr);
    const data = await PM_doc.find({ 
      timestamp: { $gte: start, $lte: end } 
    }).sort({ timestamp: 1 }).select('active_power_total timestamp');

    if (!data.length) {
      console.log(`⚠️ No data found for ${dateStr}`);
      return;
    }

    // คำนวณพลังงานรวม
    let totalEnergyKwh = 0;
    let totalPowerSum = 0;

    for (let i = 0; i < data.length; i++) {
      const p = docPower(data[i]);
      totalPowerSum += p;
      
      if (i === 0) continue;
      
      const prevP = docPower(data[i-1]);
      const intervalHours = (data[i].timestamp - data[i-1].timestamp) / 1000 / 3600;
      totalEnergyKwh += ((p + prevP) / 2) * intervalHours;
    }

    totalEnergyKwh = Number(totalEnergyKwh.toFixed(2));
    const electricityBill = calculateBill(totalEnergyKwh);
    const samples = data.length;

    console.log(`✅ Daily Bill: ${totalEnergyKwh} Unit = ${electricityBill} THB (${samples} samples)`);

    // ส่ง Push Notification และบันทึก
    await sendPushNotification(
      '💰 Daily Energy Report',
    
      {
        date: dateStr,
        energy_kwh: totalEnergyKwh,
        electricity_bill: electricityBill,
        rate_per_kwh: 4.4
      }
    );

    console.log(`📤 Daily bill notification sent for ${dateStr}`);

  } catch (err) {
    console.error('❌ Error sending daily bill notification:', err);
  }
}

// ตั้งเวลาให้ทำงานทุกวันตอนตี 1 (01:00:00)
cron.schedule('0 0 1 * * *', () => {
  console.log('⏰ Running daily bill notification job at 1:00 AM');
  sendDailyBillNotification();
}, {
  timezone: "Asia/Bangkok"
});

// ================== TEST PUSH ==================
app.get('/api/test-push', async (req, res) => {
  try {
    await sendPushNotification(
      '🔔 Test Push',
      'การแจ้งเตือนทดสอบทำงานแล้ว!',
      'test',
      {}
    );
    res.send('✅ Push sent and saved to DB');
  } catch (err) {
    console.error('❌ test-push error:', err);
    res.status(500).send('❌ Failed to send test push');
  }
});

// ================== NOTIFICATION API ==================

// 1. ดึง Peak Notifications
app.get('/api/notifications/peak', async (req, res) => {
  try {
    const { limit = 50, page = 1, unreadOnly = false } = req.query;
    const query = unreadOnly === 'true' ? { read: false } : {};
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const notifications = await PeakNotificationH.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip);

    const total = await PeakNotificationH.countDocuments(query);
    const unreadCount = await PeakNotificationH.countDocuments({ read: false });

    res.json({
      success: true,
      stats: {
        total,
        unread,
        read: total - unread,
        byType: {
          peak: {
            total: totalPeak,
            unread: unreadPeak,
            read: totalPeak - unreadPeak,
            latest: latestPeak
          },
          daily_diff: {
            total: totalDailyDiff,
            unread: unreadDailyDiff,
            read: totalDailyDiff - unreadDailyDiff,
            latest: latestDailyDiff
          },
          daily_bill: {
            total: totalDailyBill,
            unread: unreadDailyBill,
            read: totalDailyBill - unreadDailyBill,
            latest: latestDailyBill
          },
          test: {
            total: totalTest,
            unread: unreadTest,
            read: totalTest - unreadTest,
            latest: latestTest
          }
        }
      }
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/stats error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ================= Graceful Shutdown =================
process.on('SIGTERM', async () => {
    console.log('🔄 SIGTERM received, closing server...');
    await mongoose.connection.close();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('🔄 SIGINT received, closing server...');
    await mongoose.connection.close();
    process.exit(0);
});

// 2. ดึง Daily Diff Notifications
app.get('/api/notifications/daily-diff', async (req, res) => {
  try {
    const { limit = 50, page = 1, unreadOnly = false } = req.query;
    const query = unreadOnly === 'true' ? { read: false } : {};
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const notifications = await DailyDiffNotificationH.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip);

    const total = await DailyDiffNotificationH.countDocuments(query);
    const unreadCount = await DailyDiffNotificationH.countDocuments({ read: false });

    res.json({
      success: true,
      type: 'daily_diff',
      data: notifications,
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      },
      unreadCount
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/daily-diff error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3. ดึง Test Notifications
app.get('/api/notifications/test', async (req, res) => {
  try {
    const { limit = 50, page = 1, unreadOnly = false } = req.query;
    const query = unreadOnly === 'true' ? { read: false } : {};
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const notifications = await TestNotificationH.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip);

    const total = await TestNotificationH.countDocuments(query);
    const unreadCount = await TestNotificationH.countDocuments({ read: false });

    res.json({
      success: true,
      type: 'test',
      data: notifications,
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      },
      unreadCount
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/test error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ================= DAILY BILL NOTIFICATION API =================

// ดึง Daily Bill Notifications
app.get('/api/notifications/daily-bill', async (req, res) => {
  try {
    const { limit = 50, page = 1, unreadOnly = false } = req.query;
    const query = unreadOnly === 'true' ? { read: false } : {};
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const notifications = await DailyBillNotificationH.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip);

    const total = await DailyBillNotificationH.countDocuments(query);
    const unreadCount = await DailyBillNotificationH.countDocuments({ read: false });

    res.json({
      success: true,
      type: 'daily_bill',
      data: notifications,
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      },
      unreadCount
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/daily-bill error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ทดสอบส่ง Daily Bill Notification ทันที
app.get('/api/test-daily-bill', async (req, res) => {
  try {
    await sendDailyBillNotification();
    res.json({ 
      success: true, 
      message: 'Daily bill notification sent and saved to DB' 
    });
  } catch (err) {
    console.error('❌ test-daily-bill error:', err);
    res.status(500).json({ 
      success: false, 
      error: err.message 
    });
  }
});

// 4. ดึงทั้งหมด (รวม 4 collections)
app.get('/api/notifications/all', async (req, res) => {
  try {
    const { limit = 50, page = 1, unreadOnly = false } = req.query;
    const query = unreadOnly === 'true' ? { read: false } : {};
    const skip = (parseInt(page) - 1) * parseInt(limit);


    // Use ...LIB collections instead
    const PeakNotificationLIB = mongoose.model("peak_notifications_lib", peakNotificationSchema);
    const DailyDiffNotificationLIB = mongoose.model("daily_diff_notifications_lib", dailyDiffNotificationSchema);
    const DailyBillNotificationLIB = mongoose.model("daily_bill_notifications_lib", dailyBillNotificationSchema);
    const TestNotificationLIB = mongoose.model("test_notifications_lib", testNotificationSchema);

    const peakNoti = await PeakNotificationLIB.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip)
      .lean();
    
    const dailyDiffNoti = await DailyDiffNotificationLIB.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip)
      .lean();
    
    const dailyBillNoti = await DailyBillNotificationLIB.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip)
      .lean();
    
    const testNoti = await TestNotificationLIB.find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .skip(skip)
      .lean();

    const allNotifications = [
      ...peakNoti.map(n => ({...n, type: 'peak'})),
      ...dailyDiffNoti.map(n => ({...n, type: 'daily_diff'})),
      ...dailyBillNoti.map(n => ({...n, type: 'daily_bill'})),
      ...testNoti.map(n => ({...n, type: 'test'}))
    ].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
     .slice(0, parseInt(limit));

    const totalPeak = await PeakNotificationLIB.countDocuments(query);
    const totalDailyDiff = await DailyDiffNotificationLIB.countDocuments(query);
    const totalDailyBill = await DailyBillNotificationLIB.countDocuments(query);
    const totalTest = await TestNotificationLIB.countDocuments(query);
    const total = totalPeak + totalDailyDiff + totalDailyBill + totalTest;

    const unreadPeak = await PeakNotificationLIB.countDocuments({ read: false });
    const unreadDailyDiff = await DailyDiffNotificationLIB.countDocuments({ read: false });
    const unreadDailyBill = await DailyBillNotificationLIB.countDocuments({ read: false });
    const unreadTest = await TestNotificationLIB.countDocuments({ read: false });
    const unreadCount = unreadPeak + unreadDailyDiff + unreadDailyBill + unreadTest;

    res.json({
      success: true,
      data: allNotifications,
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      },
      unreadCount,
      breakdown: {
        peak: { total: totalPeak, unread: unreadPeak },
        daily_diff: { total: totalDailyDiff, unread: unreadDailyDiff },
        daily_bill: { total: totalDailyBill, unread: unreadDailyBill },
        test: { total: totalTest, unread: unreadTest }
      }
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/all error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 5. ดึงล่าสุด (รวมทุก type)
app.get('/api/notifications/recent', async (req, res) => {
  try {
    const { limit = 10 } = req.query;
    
    const peakNoti = await PeakNotificationH.find()
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .lean();
    
    const dailyDiffNoti = await DailyDiffNotificationH.find()
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .lean();
    
    const dailyBillNoti = await DailyBillNotificationH.find()
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .lean();
    
    const testNoti = await TestNotificationH.find()
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .lean();

    const allNotifications = [
      ...peakNoti.map(n => ({...n, type: 'peak'})),
      ...dailyDiffNoti.map(n => ({...n, type: 'daily_diff'})),
      ...dailyBillNoti.map(n => ({...n, type: 'daily_bill'})),
      ...testNoti.map(n => ({...n, type: 'test'}))
    ].sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
     .slice(0, parseInt(limit));

    const unreadPeak = await PeakNotificationH.countDocuments({ read: false });
    const unreadDailyDiff = await DailyDiffNotificationH.countDocuments({ read: false });
    const unreadDailyBill = await DailyBillNotificationH.countDocuments({ read: false });
    const unreadTest = await TestNotificationH.countDocuments({ read: false });
    const unreadCount = unreadPeak + unreadDailyDiff + unreadDailyBill + unreadTest;

    res.json({
      success: true,
      data: allNotifications,
      unreadCount,
      breakdown: {
        peak: unreadPeak,
        daily_diff: unreadDailyDiff,
        daily_bill: unreadDailyBill,
        test: unreadTest
      }
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/recent error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 6. ทำเครื่องหมายว่าอ่านแล้ว
app.patch('/api/notifications/mark-read', async (req, res) => {
  try {
    const { type, ids } = req.body;

    if (!type || !ids || !Array.isArray(ids)) {
      return res.status(400).json({ 
        success: false, 
        error: 'type and ids array are required',
        example: { type: 'peak', ids: ['id1', 'id2'] }
      });
    }

    let result;
    switch(type) {
      case 'peak':
        result = await PeakNotificationH.updateMany(
          { _id: { $in: ids } },
          { $set: { read: true } }
        );
        break;
      case 'daily_diff':
        result = await DailyDiffNotificationH.updateMany(
          { _id: { $in: ids } },
          { $set: { read: true } }
        );
        break;
      case 'daily_bill':
        result = await DailyBillNotificationH.updateMany(
          { _id: { $in: ids } },
          { $set: { read: true } }
        );
        break;
      case 'test':
        result = await TestNotificationH.updateMany(
          { _id: { $in: ids } },
          { $set: { read: true } }
        );
        break;
      default:
        return res.status(400).json({ success: false, error: 'Invalid type' });
    }

    res.json({
      success: true,
      message: `Marked ${result.modifiedCount} ${type} notifications as read`,
      modifiedCount: result.modifiedCount
    });
  } catch (err) {
    console.error('❌ PATCH /api/notifications/mark-read error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 7. ทำเครื่องหมายทั้งหมดว่าอ่านแล้ว
app.patch('/api/notifications/mark-all-read', async (req, res) => {
  try {
    const resultPeak = await PeakNotificationH.updateMany(
      { read: false },
      { $set: { read: true } }
    );
    
    const resultDailyDiff = await DailyDiffNotificationH.updateMany(
      { read: false },
      { $set: { read: true } }
    );
    
    const resultDailyBill = await DailyBillNotificationH.updateMany(
      { read: false },
      { $set: { read: true } }
    );
    
    const resultTest = await TestNotificationH.updateMany(
      { read: false },
      { $set: { read: true } }
    );

    const totalModified = resultPeak.modifiedCount + 
                         resultDailyDiff.modifiedCount + 
                         resultDailyBill.modifiedCount +
                         resultTest.modifiedCount;

    res.json({
      success: true,
      message: `Marked ${totalModified} notifications as read`,
      breakdown: {
        peak: resultPeak.modifiedCount,
        daily_diff: resultDailyDiff.modifiedCount,
        daily_bill: resultDailyBill.modifiedCount,
        test: resultTest.modifiedCount
      },
      totalModified
    });
  } catch (err) {
    console.error('❌ PATCH /api/notifications/mark-all-read error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 8. ลบ notification
app.delete('/api/notifications/:type/:id', async (req, res) => {
  try {
    const { type, id } = req.params;

    let result;
    switch(type) {
      case 'peak':
        result = await PeakNotificationH.findByIdAndDelete(id);
        break;
      case 'daily_diff':
        result = await DailyDiffNotificationH.findByIdAndDelete(id);
        break;
      case 'daily_bill':
        result = await DailyBillNotificationH.findByIdAndDelete(id);
        break;
      case 'test':
        result = await TestNotificationH.findByIdAndDelete(id);
        break;
      default:
        return res.status(400).json({ success: false, error: 'Invalid type' });
    }

    if (!result) {
      return res.status(404).json({ success: false, error: 'Notification not found' });
    }

    res.json({
      success: true,
      message: `${type} notification deleted successfully`
    });
  } catch (err) {
    console.error('❌ DELETE /api/notifications error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 9. ลบทั้งหมด (ตาม type หรือทุก type)
app.delete('/api/notifications', async (req, res) => {
  try {
    const { type } = req.query;

    let resultPeak, resultDailyDiff, resultDailyBill, resultTest;

    if (!type || type === 'all') {
      resultPeak = await PeakNotificationH.deleteMany({});
      resultDailyDiff = await DailyDiffNotificationH.deleteMany({});
      resultDailyBill = await DailyBillNotificationH.deleteMany({});
      resultTest = await TestNotificationH.deleteMany({});
    } else {
      switch(type) {
        case 'peak':
          resultPeak = await PeakNotificationH.deleteMany({});
          break;
        case 'daily_diff':
          resultDailyDiff = await DailyDiffNotificationH.deleteMany({});
          break;
        case 'daily_bill':
          resultDailyBill = await DailyBillNotificationH.deleteMany({});
          break;
        case 'test':
          resultTest = await TestNotificationH.deleteMany({});
          break;
        default:
          return res.status(400).json({ success: false, error: 'Invalid type' });
      }
    }

    const totalDeleted = (resultPeak?.deletedCount || 0) + 
                        (resultDailyDiff?.deletedCount || 0) + 
                        (resultDailyBill?.deletedCount || 0) +
                        (resultTest?.deletedCount || 0);

    res.json({
      success: true,
      message: `Deleted ${totalDeleted} notifications`,
      breakdown: {
        peak: resultPeak?.deletedCount || 0,
        daily_diff: resultDailyDiff?.deletedCount || 0,
        daily_bill: resultDailyBill?.deletedCount || 0,
        test: resultTest?.deletedCount || 0
      },
      totalDeleted
    });
  } catch (err) {
    console.error('❌ DELETE /api/notifications error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 10. สถิติ notification (แยกตาม type)
app.get('/api/notifications/stats', async (req, res) => {
  try {
    const totalPeak = await PeakNotification.countDocuments();
    const unreadPeak = await PeakNotification.countDocuments({ read: false });
    const latestPeak = await PeakNotification.findOne().sort({ timestamp: -1 });

    const totalDailyDiff = await DailyDiffNotification.countDocuments();
    const unreadDailyDiff = await DailyDiffNotification.countDocuments({ read: false });
    const latestDailyDiff = await DailyDiffNotification.findOne().sort({ timestamp: -1 });

    const totalDailyBill = await DailyBillNotification.countDocuments();
    const unreadDailyBill = await DailyBillNotification.countDocuments({ read: false });
    const latestDailyBill = await DailyBillNotification.findOne().sort({ timestamp: -1 });

    const totalTest = await TestNotification.countDocuments();
    const unreadTest = await TestNotification.countDocuments({ read: false });
    const latestTest = await TestNotification.findOne().sort({ timestamp: -1 });

    const total = totalPeak + totalDailyDiff + totalDailyBill + totalTest;
    const unread = unreadPeak + unreadDailyDiff + unreadDailyBill + unreadTest;

    res.json({
      success: true,
      stats: {
        total,
        unread,
        read: total - unread,
        byType: {
          peak: {
            total: totalPeak,
            unread: unreadPeak,
            read: totalPeak - unreadPeak,
            latest: latestPeak
          },
          daily_diff: {
            total: totalDailyDiff,
            unread: unreadDailyDiff,
            read: totalDailyDiff - unreadDailyDiff,
            latest: latestDailyDiff
          },
          daily_bill: {
            total: totalDailyBill,
            unread: unreadDailyBill,
            read: totalDailyBill - unreadDailyBill,
            latest: latestDailyBill
          },
          test: {
            total: totalTest,
            unread: unreadTest,
            read: totalTest - unreadTest,
            latest: latestTest
          }
        }
      }
    });
  } catch (err) {
    console.error('❌ GET /api/notifications/stats error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});
// ================= Graceful Shutdown =================
process.on('SIGTERM', async () => {
    console.log('🔄 SIGTERM received, closing server...');
    await mongoose.connection.close();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('🔄 SIGINT received, closing server...');
    await mongoose.connection.close();
    process.exit(0);
});

// ================= Start Server =================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📍 Health check: http://localhost:${PORT}/`);
});

// GET /daily-energy/:source
// Example: /daily-energy/px_pm3250?date=2025-11-16
// source can be: 'px_pm3250', 'pm_doc'
app.get('/daily-energy/:source', async (req, res) => {
  try {
    const source = req.params.source;
    const queryDate = req.query.date || new Date().toISOString().slice(0,10); // YYYY-MM-DD
    if (!/^\d{4}-\d{2}-\d{2}$/.test(queryDate)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    let Model;
    // Map legacy and current tokens to the single pm_doc collection
    if (source === 'px_pm3250' || source === 'pm_doc') Model = PM_doc;
    else return res.status(400).json({ error: 'Unknown source. Use pm_doc' });

    const start = new Date(`${queryDate}T00:00:00Z`);
    const end = new Date(`${queryDate}T23:59:59Z`);
    const limit = parseInt(req.query.limit) || 10000;

    // Return full documents in the legacy format so frontend doesn't need changes
    const docs = await Model.find({ timestamp: { $gte: start, $lte: end } })
      .sort({ timestamp: 1 })
      .limit(limit)
      .select('voltage current active_power_total timestamp mac_address');

    res.json({ message: 'Data retrieved successfully', data: docs });
  } catch (err) {
    console.error('❌ GET /daily-energy/:source error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Browser-friendly endpoint: GET /esp/:source
// - Returns JSON by default: { success, count, data }
// - If the client accepts text/html (e.g. when you paste the URL in a browser),
//   it will return a simple HTML table so you can see recent ESP data directly.
app.get('/esp/:source', async (req, res) => {
  try {
    const source = req.params.source || '';
    const limit = Math.min(1000, parseInt(req.query.limit) || 20);

    // map URL source to the unified pm_doc model
    const s = source.toLowerCase();
    let Model = null;
    if (s === 'pm_doc' || s === 'pm-doc' || s === 'px_pm3250') Model = PM_doc;
    else return res.status(404).json({ error: 'Unknown source. Use pm_doc' });

    const docs = await Model.find().sort({ timestamp: -1 }).limit(limit).lean();

    const accept = (req.get('accept') || '').toLowerCase();
    if (accept.includes('text/html')) {
      // simple HTML view for quick browser checks
      let html = `<!doctype html><html><head><meta charset="utf-8"><title>${source} recent</title>`;
      html += `<style>body{font-family:Arial,Helvetica,sans-serif}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ccc;padding:6px;text-align:left}pre{white-space:pre-wrap;word-break:break-word}</style>`;
      html += `</head><body><h2>Recent ${source} (${docs.length})</h2>`;
      html += `<p>Limit: ${limit} — <small>use <code>?limit=50</code> to change</small></p>`;
      html += `<table><thead><tr><th>timestamp</th><th>mac_address</th><th>active_power_total</th><th>raw / extra</th></tr></thead><tbody>`;
      for (const d of docs) {
        // Show timestamp exactly as stored (no timezone shift, include trailing Z)
        const ts = d.timestamp ? new Date(d.timestamp).toISOString() : '';
        const mac = d.mac_address || '';
        const power = (d.active_power_total !== undefined && d.active_power_total !== null) ? d.active_power_total : '';
        const extra = Object.assign({}, d.raw || {});
        // include any extra keys that are present on the document but not primary columns
        const extras = {};
        for (const k of Object.keys(d)) {
          if (!['_id','timestamp','mac_address','active_power_total','power','raw','__v','createdAt','updatedAt'].includes(k)) extras[k] = d[k];
        }
        if (Object.keys(extras).length) Object.assign(extra, extras);
        html += `<tr><td>${ts}</td><td>${mac}</td><td>${power}</td><td><pre>${JSON.stringify(extra,null,2)}</pre></td></tr>`;
      }
      html += `</tbody></table></body></html>`;
      return res.send(html);
    }

    // default: JSON — return only totals and basic metadata
    const jsonData = docs.map(d => ({
      timestamp: d.timestamp ? new Date(d.timestamp).toISOString() : null,
      mac_address: d.mac_address || null,
      voltage: d.voltage || null,
      current: d.current || null,
      active_power_total: d.active_power_total !== undefined && d.active_power_total !== null ? d.active_power_total : null
    }));
    res.json({ success: true, count: jsonData.length, data: jsonData });
  } catch (err) {
    console.error('❌ GET /esp/:source error:', err);
    res.status(500).json({ error: err.message });
  }
});