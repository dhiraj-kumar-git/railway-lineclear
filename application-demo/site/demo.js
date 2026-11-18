/* LineClear Control Office: the application demo page.

   Every call goes to the control-office server on this laptop, on one origin:
     /api/...        the four public REST APIs, forwarded unchanged to the load balancer
     /passenger/...  the Passenger Notification Lambda's function URL
     /staff/...      staff actions; the server adds the staff key, the page never sees it
     /cluster/...    the two autoscalers, read with kubectl
*/
"use strict";

const SECTIONS = ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10"];
const STATIONS = ["Mumbai Central", "Borivali", "Surat", "Vadodara", "Godhra", "Ratlam", "Nagda", "Kota",
  "Sawai Madhopur", "Mathura", "New Delhi"]; // labels for orientation; the services know S1 to S10
const KIND_NAMES = { rail_fracture: "Rail fracture", signal_failure: "Signal failure", track_obstruction: "Track obstruction", overhead_wire_fault: "Overhead wire fault" };
const PARTICIPANTS = { "track-availability": "Track Availability", "incident-manager": "Incident Manager" };
const PREPARES = { "track-availability": "hold the closed section and the loop line", "incident-manager": "mark the incident as being handled" };
const SERVICE_FOR = [["/api/trains", "Train Registry"], ["/api/positions", "Position Ingestion"], ["/api/incidents", "Incident Manager"],
  ["/api/schedule", "Schedule Manager"], ["/passenger", "Passenger Notification, Lambda"], ["/staff/incidents", "Incident Manager, staff key"],
  ["/staff/fault-switch", "Schedule Manager, staff key"], ["/staff/reset", "Track Availability and Incident Manager"], ["/cluster", "Kubernetes autoscalers"]];
const LEADER_SILENT_MS = 6000;
const VIEWS = ["report", "control", "passenger", "scale"];

/* ---------------------------------------------------------------- helpers */

const $ = (id) => document.getElementById(id);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const loopFor = (section) => (Number(section.slice(1)) <= 5 ? "L1" : "L2");
const stretch = (section) => { const i = SECTIONS.indexOf(section); return i >= 0 ? `${STATIONS[i]} to ${STATIONS[i + 1]}` : section; };
const serviceFor = (path) => (SERVICE_FOR.find(([p]) => path.startsWith(p)) || [, ""])[1];
const mech = (name) => `<span class="mech">${esc(name)}</span>`;
const tag = (letter) => `<span class="tag">${letter}</span>`;

function parseTime(text) {
  if (!text) return null;
  const d = new Date(String(text).replace(/(\.\d{3})\d+/, "$1"));
  return Number.isNaN(d.getTime()) ? null : d;
}
const clock = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Kolkata", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
const ist = (text) => { const d = text instanceof Date ? text : parseTime(text); return d ? clock.format(d) : ""; };
const gap = (a, b) => { const x = parseTime(a), y = parseTime(b); return x && y ? (y - x) / 1000 : null; };
function ageText(age) {
  if (typeof age !== "number") return "";
  if (age < 90) return `${Math.round(age)} s ago`;
  if (age < 5400) return `${Math.round(age / 60)} min ago`;
  return `${Math.round(age / 3600)} h ago`;
}
function shortReplica(name) { return name ? name.split("-").slice(-1)[0] : "?"; }

let toastTimer;
function toast(message, kind = "ok") {
  const box = $("toast");
  box.textContent = message;
  box.dataset.kind = kind;
  box.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { box.hidden = true; }, 7000);
}

/* ---------------------------------------------------------------- calls */

class ApiError extends Error { constructor(status, detail) { super(detail); this.status = status; } }
const activity = [];
let lastOk = 0, lastProblem = null;

async function call(path, { method = "GET", body, auto = false, text = false } = {}) {
  const started = performance.now();
  const headers = { Accept: text ? "text/plain" : "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  let response;
  try {
    response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  } catch {
    note({ method, path, status: 0, ms: Math.round(performance.now() - started), auto });
    lastProblem = { message: `No answer for ${path.split("?")[0]}`, at: Date.now() };
    throw new ApiError(0, "The control-office server did not answer. Is it still running?");
  }
  const raw = await response.text();
  note({ method, path, status: response.status, ms: Math.round(performance.now() - started), auto });
  let data = null;
  if (!text) { try { data = raw ? JSON.parse(raw) : null; } catch { data = null; } }
  if (!response.ok) {
    let detail = data && data.detail !== undefined ? data.detail : raw.slice(0, 200);
    if (Array.isArray(detail)) detail = detail.map((d) => d.msg || JSON.stringify(d)).join("; ");
    if (response.status >= 500) lastProblem = { message: `${path.split("?")[0]} answered ${response.status}`, at: Date.now() };
    throw new ApiError(response.status, String(detail || response.statusText));
  }
  lastOk = Date.now();
  return text ? raw : data;
}

function note(entry) {
  if (entry.auto && entry.status >= 200 && entry.status < 500) return; // the regular refreshes stay out of the list
  activity.unshift({ ...entry, at: new Date(), service: serviceFor(entry.path) });
  activity.length = Math.min(activity.length, 40);
  renderActivity();
}

/* ---------------------------------------------------------------- state */

const S = {
  config: null, trains: [], positions: {}, incidents: [], diversions: [], fault: null,
  replicas: {}, lastLeader: null, vacantSince: null, termLog: [],
  follow: null,          // { incidentId, txnId, view } followed on the report view
  selected: null,        // txn selected in the control office
  details: {},           // txn id -> detail
  incidentDetails: {},   // incident id -> detail
  scalers: [], passengerTrain: "12951", armedHint: false,
};

function logTerm(text) { S.termLog.unshift({ text, at: new Date() }); S.termLog = S.termLog.slice(0, 10); }

function noteCoordinator(answer) {
  const now = Date.now();
  S.replicas[answer.replica] = { ...answer, seen: now };
  if (!answer.is_leader) return;
  const prev = S.lastLeader;
  if (!prev || answer.term > prev.term || (answer.term === prev.term && answer.replica !== prev.replica)) {
    logTerm(prev ? `Term ${answer.term}: ${shortReplica(answer.replica)} took over from ${shortReplica(prev.replica)} (term ${prev.term})`
      : `Term ${answer.term}: ${shortReplica(answer.replica)} on duty`);
    S.lastLeader = { replica: answer.replica, term: answer.term, at: now };
  } else S.lastLeader.at = now;
  if (S.vacantSince) { logTerm(`Duty taken again after ${((now - S.vacantSince) / 1000).toFixed(1)} s without a controller`); S.vacantSince = null; }
}

function duty() {
  const lead = S.lastLeader;
  if (!lead) return { known: false, onDuty: false };
  const silent = Date.now() - lead.at;
  const onDuty = silent < LEADER_SILENT_MS;
  if (!onDuty && !S.vacantSince) { S.vacantSince = lead.at; logTerm(`No controller on duty: ${shortReplica(lead.replica)} stopped renewing term ${lead.term}`); }
  return { known: true, onDuty, replica: lead.replica, term: lead.term, silent };
}

const openIncident = (id) => { const i = S.incidents.find((x) => x.incident_id === id); return i && i.status !== "RESOLVED" ? i : null; };

/* ---------------------------------------------------------------- refreshing */

function every(ms, task, when = () => true) {
  let busy = false;
  const tick = async () => {
    if (busy || document.hidden || !when()) return;
    busy = true;
    try { await task(); } catch { /* shown by the lamp */ } finally { busy = false; }
  };
  tick();
  return setInterval(tick, ms);
}

async function refreshTrains() { S.trains = await call("/api/trains", { auto: true }); renderTrainPickers(); }
async function refreshCoordinator() { noteCoordinator(await call("/api/schedule/coordinator", { auto: true })); renderDuty(); }
async function refreshRegisters() {
  const [incidents, diversions, fault] = await Promise.all([
    call("/api/incidents", { auto: true }), call("/api/schedule/diversions", { auto: true }), call("/api/schedule/fault-switch", { auto: true })]);
  S.incidents = incidents; S.diversions = diversions; S.fault = fault;
  renderIncidents(); renderDiversions(); renderTrack(); renderFault();
}
async function refreshPositions() {
  if (!S.trains.length) await refreshTrains();
  const answers = await Promise.allSettled(S.trains.map((t) => call(`/api/positions/${t.number}`, { auto: true })));
  answers.forEach((a, i) => {
    const n = S.trains[i].number;
    if (a.status === "fulfilled") S.positions[n] = a.value; else if (a.reason.status === 404) delete S.positions[n];
  });
  renderPositions(); renderTrack();
}
async function refreshScalers() {
  try { S.scalers = await call("/cluster/autoscalers", { auto: true }); $("scaler-source").innerHTML = S.config.mode === "stub" ? "Simulated autoscalers that follow the GPS report rate." : "From <code>kubectl get hpa -n lineclear</code>, every 5 s."; }
  catch (e) { $("scalers").innerHTML = `<p class="hint">${esc(e.message)}</p>`; return; }
  renderScalers();
}

/** Detail of a diversion; finished ones are read once and kept. */
async function diversion(txnId) {
  const known = S.details[txnId];
  if (known && finished(known)) return known;
  const d = await call(`/api/schedule/diversions/${encodeURIComponent(txnId)}`, { auto: true });
  S.details[txnId] = d;
  return d;
}
async function incident(id) {
  const known = S.incidentDetails[id];
  if (known && known.status === "RESOLVED") return known;
  const d = await call(`/api/incidents/${encodeURIComponent(id)}`, { auto: true });
  S.incidentDetails[id] = d;
  return d;
}
const finished = (t) => t && t.completed_at && (t.state === "ABORT" || t.notified_at);

/* ---------------------------------------------------------------- the 2PC timeline */

function timeline(inc, txn) {
  const steps = [];
  const d = duty();
  const train = inc && inc.train_number;
  steps.push({
    status: inc ? "done" : "now", n: 1, time: inc && inc.reported_at,
    title: inc ? `Incident ${inc.incident_id} saved: ${KIND_NAMES[inc.kind] || inc.kind} on ${inc.section}` : "Saving the incident",
    how: `${mech("REST")} <code>POST /api/incidents</code> to <b>Incident Manager</b>, stored in its own database in ${mech("RDS")} ${tag("b")} ${tag("g")}`,
  });
  if (inc && !train) {
    steps.push({ status: "skip", n: 2, title: "No train named, so nothing to divert", how: "The incident is on record and blocks the section on the diagram; no transaction starts." });
    return steps;
  }
  steps.push({
    status: txn ? "done" : "now", n: 2,
    title: "Announced to the duty controller",
    how: `Incident Manager publishes <code>incident_reported</code> on ${mech("Kafka")} <code>incident.events</code>; the leading Schedule Manager replica consumes it ${tag("b")}`,
  });
  const waiting = !txn && d.known && !d.onDuty;
  steps.push({
    status: txn ? "done" : waiting ? "fail" : "now", n: 3, time: txn && txn.started_at,
    title: txn ? `Duty controller ${shortReplica(txn.coordinator_id)} starts transaction ${txn.txn_id} in term ${txn.term}`
      : waiting ? "Waiting: no duty controller right now" : "Duty controller picking it up",
    how: txn ? `Looks up train ${esc(txn.train_number)}'s route in <b>Train Registry</b> over ${mech("REST")}, then plans the diversion: close ${txn.blocked_section}, run through loop line ${txn.loop_line} ${tag("f")}`
      : waiting ? "The lease ran out. The other replica wins the next term, reads the log (recovery) and then takes this incident." : "",
  });
  if (!txn) return steps;
  const votes = txn.participants || [];
  const allVoted = votes.length && votes.every((p) => p.vote);
  steps.push({
    status: allVoted ? "done" : "now", n: 4,
    title: "Phase 1: prepare, asking each participant for line clear",
    how: `PREPARING is written to the coordinator's log in ${mech("RDS")} <i>before</i> asking. Then ${mech("gRPC")} <code>Prepare</code> to both, in parallel, 2 s deadline each; no answer counts as NO ${tag("f")}`,
    extra: `<div class="votes">${votes.map((p) => `<div class="vote"><b>${esc(PARTICIPANTS[p.participant] || p.participant)}</b>
      ${p.vote ? `<span class="state" data-state="${p.vote}">${p.vote}</span>` : `<span class="state" data-state="PREPARING">asking</span>`}
      <span class="hint">${p.vote === "YES" ? `prepared to ${esc(PREPARES[p.participant] || "do its part")}, vote saved first` : esc(p.reason || "")}</span></div>`).join("")}</div>`,
  });
  const decided = txn.state === "COMMIT" || txn.state === "ABORT";
  const took = gap(txn.started_at, txn.decided_at);
  steps.push({
    status: !decided ? "now" : txn.state === "COMMIT" ? "done" : "fail", n: 5, time: txn.decided_at,
    title: decided ? `Decision: ${txn.state}${took !== null ? ` (phase 1 took ${took.toFixed(1)} s)` : ""}` : "Deciding",
    how: decided ? (txn.state === "COMMIT" ? "Both said YES. The decision and the revised times are logged first, then published" : `One said NO: ${esc(txn.reason || "")}. Logged, then published`)
      + ` on ${mech("Kafka")} <code>coordination.events</code> ${tag("f")}` : "",
    extra: txn.state === "COMMIT" && (txn.revised_stops || []).length ? `<table class="register stops"><thead><tr><th>Station</th><th>Planned</th><th>Revised</th></tr></thead><tbody>${
      txn.revised_stops.map((s) => `<tr><td>${esc(s.station_name)}</td><td>${esc(s.planned_arrival)}</td><td class="late">${esc(s.revised_arrival)} (+${s.delay_minutes} min)</td></tr>`).join("")}</tbody></table>` : "",
  });
  if (!decided) return steps;
  const acked = votes.length && votes.every((p) => p.acknowledged_at);
  const late = gap(txn.decided_at, txn.completed_at);
  steps.push({
    status: acked || txn.completed_at ? "done" : "now", n: 6, time: txn.completed_at,
    title: `Phase 2: ${txn.state === "COMMIT" ? "commit" : "abort"} delivered to both participants`,
    how: `${mech("gRPC")} <code>${txn.state === "COMMIT" ? "Commit" : "Abort"}</code> to Track Availability and Incident Manager, retried until both acknowledge. Orders from an older term are refused ${tag("f")}`
      + (late !== null && late > 4 ? `<br><b>Finished ${late.toFixed(0)} s after the decision:</b> the coordinator froze after logging it, and the next duty controller found it in the log (recovery) and completed phase 2.` : ""),
  });
  if (txn.state === "COMMIT") {
    steps.push({
      status: txn.notified_at ? "done" : "now", n: 7, time: txn.notified_at,
      title: "Passengers told",
      how: `One message on ${mech("SQS")} wakes the Passenger Notification ${mech("Lambda")}, which writes the notice to ${mech("DynamoDB")} and the bulletin to ${mech("S3")} ${tag("b")} ${tag("g")}`,
    });
  } else {
    steps.push({ status: "done", n: 7, title: "Nothing changed anywhere", how: "Both participants released what they had prepared. No hold, no status change, no passenger notice: all or nothing." });
  }
  return steps;
}

function timelineHTML(inc, txn) {
  const steps = timeline(inc, txn);
  const head = txn ? `<div class="timeline-head"><h4>${esc(txn.txn_id)}</h4>
      <span class="state" data-state="${esc(txn.state)}">${esc(txn.state)}</span>
      <span class="hint">Train ${esc(txn.train_number)} · ${esc(txn.blocked_section)} closed · via ${esc(txn.loop_line)} · term ${esc(txn.term)} · coordinator ${esc(shortReplica(txn.coordinator_id))}</span></div>` : "";
  const tools = txn ? `<div class="timeline-tools">
      <a class="secondary" href="${lokiLink(txn.txn_id)}" target="_blank" rel="noopener">Logs for ${esc(txn.txn_id)} in Grafana</a>
      <a class="secondary" href="${dash("lineclear-railway-operations")}" target="_blank" rel="noopener">Railway operations dashboard</a>
      ${txn.state === "COMMIT" ? `<a class="secondary" href="#passenger" data-passenger="${esc(txn.train_number)}">Passenger view</a>` : ""}</div>` : "";
  return head + `<ol class="timeline">${steps.map((s) => `<li class="tl-step" data-status="${s.status}">
      <div class="tl-mark"><span class="tl-dot">${s.status === "fail" ? "!" : s.status === "done" ? "✓" : s.n}</span><span class="tl-line"></span></div>
      <div class="tl-body"><div class="tl-title">${s.title}${s.time ? `<span class="tl-time">${ist(s.time)}</span>` : ""}</div>
      ${s.how ? `<div class="tl-how">${s.how}</div>` : ""}${s.extra || ""}</div></li>`).join("")}</ol>` + tools;
}

/* ---------------------------------------------------------------- links to the tools */

function grafana(path) { return (S.config ? S.config.grafana : "http://localhost:3000") + path; }
function dash(uid) { return grafana(`/d/${uid}?orgId=1&from=now-15m&to=now`); }
function explore(uid, type, expr, from) {
  const panes = { a: { datasource: uid, queries: [{ refId: "A", expr, editorMode: "code", datasource: { type, uid } }], range: { from, to: "now" } } };
  return grafana(`/explore?schemaVersion=1&orgId=1&panes=${encodeURIComponent(JSON.stringify(panes))}`);
}
const lokiLink = (txnId) => explore("loki", "loki", `{app=~".+"} | json | txn_id="${txnId}"`, "now-1h");

function renderTools() {
  const txnId = (S.follow && S.follow.txnId) || S.selected;
  $("follow-note").innerHTML = txnId ? `Following <b>${esc(txnId)}</b>. One Loki query finds it in every service, because each log line carries its <code>txn_id</code>.`
    : "Select or create a diversion to follow its transaction ID through every service.";
  const argocd = S.config ? S.config.argocd : "https://localhost:8443";
  $("tool-links").innerHTML = [
    txnId && [lokiLink(txnId), `Loki: logs for ${txnId}`, "every service, the Lambda and the forwarder"],
    [dash("lineclear-railway-operations"), "Railway operations", "duty controller, terms, commits, aborts, votes"],
    [dash("lineclear-service-health"), "Service health", "requests per second, errors, latency"],
    [dash("lineclear-cluster-health"), "Cluster health", "nodes, pods, the two autoscalers"],
    [explore("prometheus", "prometheus", 'ALERTS{alertname="NoDutyController"}', "now-15m"), "Alert: NoDutyController", "pending, then firing when no leader"],
    [`${argocd}/applications`, "ArgoCD applications", "what Git has deployed"],
  ].filter(Boolean).map(([href, title, sub]) => `<a href="${href}" target="_blank" rel="noopener">${esc(title)}<small>${esc(sub)}</small></a>`).join("")
    + `<p class="hint">Grafana and ArgoCD open through the port-forwards in terminals D and E.</p>`;
}

/* ---------------------------------------------------------------- rendering */

function renderClock() {
  $("clock").textContent = clock.format(new Date());
  const lamp = $("link-state");
  const problem = lastProblem && Date.now() - lastProblem.at < 8000 ? lastProblem : null;
  if (problem) { lamp.dataset.state = "trouble"; lamp.textContent = problem.message; }
  else if (lastOk) { const ago = Math.round((Date.now() - lastOk) / 1000); lamp.dataset.state = "ok"; lamp.textContent = ago <= 2 ? "Services answering" : `Last answer ${ago} s ago`; }
}

function renderDuty() {
  const d = duty();
  const chip = $("duty-chip");
  chip.dataset.state = d.known && !d.onDuty ? "vacant" : "ok";
  chip.textContent = !d.known ? "Duty controller: waiting" : d.onDuty ? `Duty controller ${shortReplica(d.replica)}, term ${d.term}` : `No duty controller for ${(d.silent / 1000).toFixed(0)} s`;
  const rows = Object.values(S.replicas).sort((a, b) => a.replica.localeCompare(b.replica)).map((r) => {
    const leads = d.onDuty && r.replica === d.replica;
    return `<div class="replica" data-lead="${leads}"><span><b>${leads ? "On duty" : "Standing by"}</b> <code>${esc(r.replica)}</code></span><span>term ${esc(r.term)}</span></div>`;
  }).join("");
  $("duty").innerHTML = (d.known ? (d.onDuty ? `<div><span class="term-big">Term ${d.term}</span> <span class="hint">held by ${esc(shortReplica(d.replica))}</span></div>`
    : `<div class="vacant">No controller on duty for ${(d.silent / 1000).toFixed(0)} s. Diversions wait until the next term starts.</div>`) : "") + rows;
  $("term-log").innerHTML = S.termLog.map((t) => `<li>${ist(t.at)} ${esc(t.text)}</li>`).join("");
}

function trackPicture() {
  const blocked = new Map(), held = new Map(), diverted = new Map();
  for (const i of S.incidents) if (i.status !== "RESOLVED") blocked.set(i.section, i);
  for (const t of S.diversions) {
    if ((t.state === "COMMIT" && openIncident(t.incident_id)) || t.state === "PREPARING") {
      if (!held.has(t.loop_line)) held.set(t.loop_line, t);
      if (t.state === "COMMIT" && !diverted.has(t.train_number)) diverted.set(t.train_number, t);
    }
  }
  return { blocked, held, diverted };
}

function renderTrack() {
  const { blocked, held, diverted } = trackPicture();
  const onSection = {}, onLoop = { L1: [], L2: [] };
  for (const [number, p] of Object.entries(S.positions)) {
    const tagHtml = `<span class="train-tag" data-stale="${(p.age_seconds || 0) > 300}" title="${esc(number)}: ${esc(ageText(p.age_seconds))}">${esc(number)}</span>`;
    const div = diverted.get(number);
    if (div) onLoop[div.loop_line].push(tagHtml); else (onSection[p.section] = onSection[p.section] || []).push(tagHtml);
  }
  const stations = `<div class="station-row">${STATIONS.map((s) => `<span title="${esc(s)}">${esc(s)}</span>`).join("")}</div>`;
  const sections = SECTIONS.map((s) => {
    const b = blocked.get(s);
    return `<div class="section" data-state="${b ? "blocked" : "clear"}" title="${esc(s)}: ${esc(stretch(s))}">
      <div class="trains">${b ? `<span class="blocked-note">${esc(b.incident_id)}</span>` : ""}${(onSection[s] || []).join("")}</div>
      <div class="rail"></div><div class="label">${s}</div></div>`;
  }).join("");
  const loops = ["L1", "L2"].map((l) => {
    const h = held.get(l);
    return `<div class="loop ${l.toLowerCase()}" data-state="${h ? "held" : "free"}"><div class="trains">${onLoop[l].join("")}</div><div class="rail"></div>
      <div class="label">Loop line ${l}${h ? `: ${h.state === "PREPARING" ? "prepared for" : "reserved for"} ${esc(h.train_number)} by ${esc(h.txn_id)}` : ", free"}</div></div>`;
  }).join("");
  $("track").innerHTML = stations + sections + loops;
}

function renderDiversions() {
  const rows = S.diversions.slice(0, 12);
  $("diversions").innerHTML = rows.length ? rows.map((t) => `<tr data-txn="${esc(t.txn_id)}" aria-selected="${t.txn_id === S.selected}">
      <td><b>${esc(t.txn_id)}</b></td><td>${esc(t.train_number)}</td><td>${esc(t.blocked_section)}</td><td>${esc(t.loop_line)}</td><td>${esc(t.term)}</td>
      <td><span class="state" data-state="${esc(t.state)}">${esc(t.state)}</span></td><td class="why">${esc(t.reason || "")}</td></tr>`).join("")
    : `<tr><td colspan="7" class="empty">No diversions yet. Report an incident with a train.</td></tr>`;
}

function renderIncidents() {
  const rows = S.incidents.slice(0, 12);
  const staffOn = S.config && S.config.staff.ready;
  $("incidents").innerHTML = rows.length ? rows.map((i) => `<tr><td><b>${esc(i.incident_id)}</b></td><td>${esc(KIND_NAMES[i.kind] || i.kind)}</td>
      <td>${esc(i.section)}</td><td>${esc(i.train_number || "")}</td><td><span class="state" data-state="${esc(i.status)}">${esc(i.status)}</span></td>
      <td>${ist(i.reported_at)}</td><td>${i.status !== "RESOLVED" && !i.pending_txn_id ? `<button type="button" class="link-button" data-resolve="${esc(i.incident_id)}" ${staffOn ? "" : "disabled"}>Resolve</button>` : ""}</td></tr>`).join("")
    : `<tr><td colspan="7" class="empty">No incidents</td></tr>`;
}

function renderFault() {
  const f = S.fault;
  $("fault-state").innerHTML = f && f.armed ? `<b>Armed now: the duty controller freezes for ${esc(f.pause_seconds)} s after its next COMMIT.</b>` : "";
}

function renderPositions() {
  $("positions").innerHTML = S.trains.map((t) => {
    const p = S.positions[t.number];
    return `<tr><td><b>${esc(t.number)}</b></td><td>${esc(t.name)}</td><td>${p ? esc(p.section) : ""}</td><td>${p && p.speed_kmph ? `${esc(p.speed_kmph)} km/h` : ""}</td>
      <td>${p ? esc(ageText(p.age_seconds)) : "No report yet"}</td></tr>`;
  }).join("") || `<tr><td colspan="5" class="empty">No trains</td></tr>`;
}

function renderScalers() {
  const names = { "position-ingestion": "Position Ingestion", "train-registry": "Train Registry" };
  $("scalers").innerHTML = S.scalers.map((s) => {
    const max = s.max || 8, cur = s.current || 0, want = Math.max(s.desired || 0, cur);
    const pods = Array.from({ length: max }, (_, i) => `<span class="pod" data-on="${i < cur}" data-wanted="${i >= cur && i < want}"></span>`).join("");
    const cpu = typeof s.cpu_percent === "number" ? s.cpu_percent : null;
    return `<div class="scaler"><h4>${esc(names[s.name] || s.name)}</h4><div class="pods">${pods}</div>
      <div><b>${cur} pods</b> <span class="hint">(${s.min} to ${max}${want > cur ? `, ${want} wanted` : ""})</span></div>
      <div class="hint">CPU ${cpu === null ? "not measured yet" : `${cpu}%`} against a ${esc(s.cpu_target || 60)}% target</div>
      <div class="cpu"><span style="width:${Math.min(100, cpu || 0)}%" data-hot="${cpu !== null && cpu > (s.cpu_target || 60)}"></span><i style="left:${s.cpu_target || 60}%"></i></div></div>`;
  }).join("") || `<p class="hint">No autoscalers found.</p>`;
}

function renderActivity() {
  $("activity").innerHTML = activity.slice(0, 18).map((a) => `<li data-bad="${a.status === 0 || a.status >= 400}">
      <b>${esc(a.method)}</b><code>${esc(a.path.split("?")[0])}</code><span>${a.status || "none"}</span>
      <span class="svc">${esc(a.service)} · ${a.ms} ms · ${ist(a.at)}</span></li>`).join("") || `<li class="hint" style="display:block;border:0">Your actions appear here.</li>`;
}

function renderTrainPickers() {
  const reportTrain = $("report-train");
  if (reportTrain.options.length <= 1) {
    for (const t of S.trains) reportTrain.insertAdjacentHTML("beforeend", `<option value="${esc(t.number)}">${esc(t.number)} ${esc(t.name)}</option>`);
  }
  const pass = $("passenger-train");
  if (!pass.options.length) {
    for (const t of S.trains) pass.insertAdjacentHTML("beforeend", `<option value="${esc(t.number)}">${esc(t.number)} ${esc(t.name)}</option>`);
    pass.value = S.passengerTrain;
  }
}

async function renderFollowed() {
  const f = S.follow;
  if (!f) return;
  const inc = await incident(f.incidentId).catch(() => null);
  if (!f.txnId && inc && inc.train_number) {
    const summary = S.diversions.find((t) => t.incident_id === f.incidentId) ||
      (await call("/api/schedule/diversions", { auto: true }).then((list) => { S.diversions = list; return list.find((t) => t.incident_id === f.incidentId); }));
    if (summary) f.txnId = summary.txn_id;
  }
  const txn = f.txnId ? await diversion(f.txnId).catch(() => null) : null;
  if (inc && !inc.train_number) f.done = true;
  $("report-follow").hidden = false;
  $("report-follow-title").textContent = txn ? `What happened to ${inc ? inc.incident_id : "the report"}: transaction ${txn.txn_id}` : `What happens next to ${inc ? inc.incident_id : "the report"}`;
  $("report-timeline").innerHTML = timelineHTML(inc, txn);
  if (S.selected === null && txn) S.selected = txn.txn_id;
  renderTools();
}

async function renderSelected() {
  if (!S.selected) { $("control-timeline").innerHTML = ""; return; }
  const txn = await diversion(S.selected).catch(() => null);
  if (!txn) return;
  const inc = await incident(txn.incident_id).catch(() => null);
  $("control-timeline").innerHTML = timelineHTML(inc, txn);
}

async function renderPassenger() {
  const train = S.passengerTrain;
  const box = $("passenger-notice");
  let notices;
  try { notices = await call(`/passenger/notices/${train}`, { auto: true }); }
  catch (e) {
    box.innerHTML = e.status === 404 ? `<div class="on-time">No diversion notices for train ${esc(train)}: it runs to the timetable.</div>`
      : `<p class="hint">${esc(e.message)}</p>`;
    return;
  }
  const [latest, ...older] = notices;
  const name = (S.trains.find((t) => t.number === train) || {}).name || "";
  box.innerHTML = `<article class="notice"><h3>Train ${esc(train)} ${esc(name)} is diverted</h3>
      <p class="route">Block section ${esc(latest.blocked_section)} is closed (incident ${esc(latest.incident_id)}), so the train runs through loop line ${esc(latest.loop_line)}.</p>
      <table><thead><tr><th>Station</th><th>Was due</th><th>Now due</th></tr></thead><tbody>${(latest.stations || []).map((s) =>
        `<tr><td>${esc(s.station_name)}</td><td>${esc(s.planned_arrival)}</td><td class="late">${esc(s.revised_arrival)} (+${esc(s.delay_minutes)} min)</td></tr>`).join("")}</tbody></table>
      <p class="hint">Notice ${esc(latest.txn_id)}, decided ${ist(latest.decided_at)}. <button type="button" class="link-button" data-bulletin="${esc(latest.txn_id)}">Show the station bulletin from S3</button></p>
      <div id="bulletin" class="bulletin" hidden></div></article>
    ${older.length ? `<p class="older">Earlier notices for this train: ${older.map((n) => esc(n.txn_id)).join(", ")}</p>` : ""}`;
}

/* ---------------------------------------------------------------- views */

let view = "report";
function showView() {
  const wanted = location.hash.replace("#", "");
  view = VIEWS.includes(wanted) ? wanted : "report";
  for (const v of VIEWS) {
    $(`view-${v}`).hidden = v !== view;
    const t = $(`tab-${v}`);
    if (v === view) t.setAttribute("aria-current", "page"); else t.removeAttribute("aria-current");
  }
  if (view === "passenger") renderPassenger();
  if (view === "scale") { refreshScalers(); refreshPositions(); }
  if (view === "control") { renderTrack(); renderSelected(); }
}

/* ---------------------------------------------------------------- actions */

async function report(payload) {
  const inc = await call("/api/incidents", { method: "POST", body: payload });
  S.incidentDetails[inc.incident_id] = inc;
  S.follow = { incidentId: inc.incident_id, txnId: null };
  toast(`${inc.incident_id} saved by Incident Manager${payload.train_number ? `; the duty controller now diverts ${payload.train_number}` : ""}`);
  refreshRegisters();
  renderFollowed();
  return inc;
}

async function waitForDecision(incidentId, seconds = 40) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    const list = await call("/api/schedule/diversions", { auto: true });
    const t = list.find((x) => x.incident_id === incidentId && x.completed_at);
    if (t) return t;
    await sleep(1000);
  }
  return null;
}

async function scenario(name) {
  const buttons = document.querySelectorAll(".scenario");
  buttons.forEach((b) => { b.disabled = true; });
  try {
    if (name === "commit") {
      await report({ kind: "rail_fracture", section: "S7", train_number: "12951", description: "Scenario A: rail fracture near Kota" });
    } else if (name === "abort") {
      const first = await report({ kind: "signal_failure", section: "S9", train_number: "12002", description: "Scenario B, first: signal failure, 12002 takes L2" });
      toast(`${first.incident_id}: waiting for 12002's diversion to commit and reserve L2`);
      const t = await waitForDecision(first.incident_id);
      if (!t) { toast("12002's diversion did not finish in 40 s; check the control office", "bad"); return; }
      toast(`${t.txn_id} ${t.state}: L2 is reserved for 12002. Now 12951 asks for L2.`);
      await sleep(1500);
      await report({ kind: "rail_fracture", section: "S7", train_number: "12951", description: "Scenario B, second: 12951 also needs L2" });
    }
  } catch (e) { toast(e.message, "bad"); }
  finally { buttons.forEach((b) => { b.disabled = false; }); }
}

async function staff(path, body, label) {
  const out = $("staff-output");
  try {
    const answer = await call(path, { method: "POST", body: body || {} });
    if (answer && answer.output) { out.hidden = false; out.textContent = answer.output.join("\n"); }
    toast(label);
    refreshRegisters();
    return answer;
  } catch (e) { toast(e.message, "bad"); return null; }
}

function bind() {
  window.addEventListener("hashchange", showView);
  $("report-section").innerHTML = SECTIONS.map((s) => `<option value="${s}" ${s === "S7" ? "selected" : ""}>${s}: ${esc(stretch(s))}</option>`).join("");
  $("report-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    const payload = { kind: form.get("kind"), section: form.get("section"), description: form.get("description") || "" };
    if (form.get("train_number")) payload.train_number = form.get("train_number");
    try { await report(payload); } catch (err) { toast(err.message, "bad"); }
  });
  document.querySelectorAll(".scenario").forEach((b) => b.addEventListener("click", () => scenario(b.dataset.scenario)));
  $("diversions").addEventListener("click", (e) => {
    const row = e.target.closest("tr[data-txn]");
    if (!row) return;
    S.selected = row.dataset.txn;
    renderDiversions(); renderSelected(); renderTools();
  });
  $("incidents").addEventListener("click", (e) => {
    const b = e.target.closest("[data-resolve]");
    if (b) staff(`/staff/incidents/${b.dataset.resolve}/resolve`, { note: "Line restored, cleared by the control office" }, `${b.dataset.resolve} resolved`);
  });
  $("arm-freeze").addEventListener("click", async () => {
    const a = await staff("/staff/fault-switch", { pause_seconds: 20 }, "Fault switch armed. Now run scenario A.");
    if (a) {
      const go = document.createElement("button");
      go.type = "button"; go.className = "primary"; go.textContent = "Run scenario A now";
      go.addEventListener("click", () => { go.remove(); scenario("commit"); location.hash = "#control"; });
      $("arm-freeze").after(go);
    }
  });
  $("free-track").addEventListener("click", async () => {
    $("free-track").disabled = true;
    await staff("/staff/reset", {}, "Track freed: holds released, open incidents resolved");
    $("free-track").disabled = false;
    S.follow = null; S.selected = null; $("report-follow").hidden = true; renderSelected(); renderTools();
  });
  $("gps-round").addEventListener("click", async () => {
    let i = Math.floor(Date.now() / 10000);
    const sends = S.trains.map((t, k) => call("/api/positions", { method: "POST", body: {
      train_number: t.number, section: SECTIONS[(i + k * 3) % 10], latitude: 22 + k * 0.5, longitude: 75 + k * 0.4, speed_kmph: 80 + k * 7 } }));
    const done = await Promise.allSettled(sends);
    toast(`${done.filter((d) => d.status === "fulfilled").length} GPS reports accepted by Position Ingestion (202), stored in DynamoDB and sent to Kafka`);
    refreshPositions();
  });
  $("rush").addEventListener("click", async () => { try { await call("/local/rush", { method: "POST", body: {} }); toast("Simulated rush hour: 12 GPS reports a second for 60 s"); } catch (e) { toast(e.message, "bad"); } });
  $("passenger-train").addEventListener("change", (e) => { S.passengerTrain = e.target.value; renderPassenger(); });
  document.addEventListener("click", async (e) => {
    const p = e.target.closest("[data-passenger]");
    if (p) { S.passengerTrain = p.dataset.passenger; $("passenger-train").value = p.dataset.passenger; }
    const b = e.target.closest("[data-bulletin]");
    if (b) {
      const box = $("bulletin");
      try { box.textContent = await call(`/passenger/bulletins/${b.dataset.bulletin}`, { text: true }); box.hidden = false; }
      catch (err) { toast(err.message, "bad"); }
    }
  });
}

async function start() {
  bind();
  try { S.config = await call("/local/config", { auto: true }); } catch { S.config = { mode: "unknown", staff: {}, reset: {}, passenger: {}, grafana: "http://localhost:3000", argocd: "https://localhost:8443" }; }
  const badge = $("mode-badge");
  badge.dataset.mode = S.config.mode;
  badge.textContent = S.config.mode === "remote" ? "Live: AWS ap-south-1" : "Simulated: no AWS";
  $("rush").hidden = S.config.mode !== "stub";
  $("staff-state").textContent = S.config.staff.ready ? "Staff key held by the control-office server (never sent to this page)."
    : `Staff actions are off: ${S.config.staff.reason}`;
  $("arm-freeze").disabled = !S.config.staff.ready;
  $("free-track").disabled = !S.config.reset.ready;
  renderTools(); renderActivity();
  showView();
  setInterval(renderClock, 1000);
  try { await refreshTrains(); } catch { /* retried by the refreshes */ }
  every(1500, refreshCoordinator);
  every(3000, refreshRegisters);
  every(5000, refreshPositions, () => view === "scale" || view === "control");
  every(5000, refreshScalers, () => view === "scale");
  every(5000, renderPassenger, () => view === "passenger");
  every(1500, renderFollowed, () => S.follow && !S.follow.done && !(S.follow.txnId && finished(S.details[S.follow.txnId])));
  every(2000, renderSelected, () => view === "control" && S.selected && !finished(S.details[S.selected]));
}

document.addEventListener("DOMContentLoaded", start);
