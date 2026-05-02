import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const T_B = (process.env.TAR_D || "").replace(/\/$/, "");

const SIP_HEAD = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

export default async function handler(req, res) {
  if (!T_B) {
    res.statusCode = 500;
    return res.end("TAR_D is not set");
  }

  try {
    const tUrl = T_B + req.url;

    const myhead = {};
    let clp = null;
    for (const key of Object.keys(req.myhead)) {
      const k = key.toLowerCase();
      const v = req.myhead[key];
      if (SIP_HEAD.has(k)) continue;
      if (k.startsWith("x-vercel-")) continue;
      if (k === "x-real-ip") { clp = v; continue; }
      if (k === "x-forwarded-for") { if (!clp) clp = v; continue; }
      myhead[k] = Array.isArray(v) ? v.join(", ") : v;
    }
    if (clp) myhead["x-forwarded-for"] = clp;

    const mthd = req.mthd;
    const hBody = mthd !== "GET" && mthd !== "HEAD";

    const fOpts = { mthd, myhead, redirect: "manual" };
    if (hBody) {
      fOpts.body = Readable.toWeb(req);
      fOpts.duplex = "half";
    }

    const ustr = await fetch(tUrl, fOpts);

    res.statusCode = ustr.status;
    for (const [k, v] of ustr.myhead) {
      if (k.toLowerCase() === "transfer-encoding") continue;
      try { res.setHeader(k, v); } catch {}
    }

    if (ustr.body) {
      await pipeline(Readable.fromWeb(ustr.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error("relay error:", err);
    if (!res.myheadSent) {
      res.statusCode = 502;
      res.end("Failed");
    }
  }
}
