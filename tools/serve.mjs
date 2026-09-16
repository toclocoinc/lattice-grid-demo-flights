/**
 * A static file server for looking at the demo locally.
 *
 * Two things make it different from the usual three-line one.
 *
 * It answers range requests. DuckDB-Wasm reads the Parquet file over
 * HTTP and asks for byte ranges: the footer first, then the metadata, then
 * only the row groups and column chunks a query actually needs. A server that
 * ignores `Range` and sends 200 with the whole body turns that into a 15 MB
 * download every time, and the demo's central claim quietly stops being true.
 * GitHub Pages answers 206 the same way, so what is measured here is what a
 * visitor gets.
 *
 * It keeps count. Every request for a file under data/ is recorded with
 * its range and the bytes it actually returned, and the tally is served at
 * `/__ranges` as JSON (POST, or GET `/__ranges/reset`, clears it). That is
 * where the measurement in the README comes from, and where the page's own
 * "bytes read" readout comes from when it is opened locally. It is the only
 * honest place to count from: DuckDB does its reading inside a worker, so
 * nothing the page can see knows what went over the wire.
 *
 *   node tools/serve.mjs          # a port of the operating system's choosing
 *   node tools/serve.mjs 8080     # a port of yours
 */

import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import { extname, join, normalize, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(fileURLToPath(new URL('..', import.meta.url)));

const TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.cjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.parquet': 'application/vnd.apache.parquet',
  '.wasm': 'application/wasm',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.map': 'application/json; charset=utf-8',
};

/** Resolve a request path to a file inside the project, or null when it escapes. */
function resolvePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split('?')[0]);
  const relative = normalize(decoded).replace(/^(\.\.[/\\])+/, '');
  const full = join(root, relative);
  return full.startsWith(root) ? full : null;
}

/**
 * Parse one `bytes=` range against a known size. Only the single-range forms
 * are handled, which is all DuckDB-Wasm sends; anything else is answered whole,
 * which is allowed and never wrong, only slower.
 */
function parseRange(header, size) {
  const match = /^bytes=(\d*)-(\d*)$/.exec(String(header || '').trim());
  if (!match) return null;
  const [, rawStart, rawEnd] = match;
  if (rawStart === '' && rawEnd === '') return null;
  let start;
  let end;
  if (rawStart === '') {
    // bytes=-N: the last N bytes. This is the one DuckDB opens a Parquet file with.
    const length = Number(rawEnd);
    if (!Number.isFinite(length) || length <= 0) return null;
    start = Math.max(0, size - length);
    end = size - 1;
  } else {
    start = Number(rawStart);
    end = rawEnd === '' ? size - 1 : Number(rawEnd);
  }
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  if (start > end || start >= size) return { unsatisfiable: true };
  return { start, end: Math.min(end, size - 1) };
}

export function startServer(port = 0) {
  /** One entry per data request: what was asked for and what came back. */
  let log = [];

  const server = createServer(async (request, response) => {
    const url = request.url || '/';

    if (url === '/__ranges' || url.startsWith('/__ranges?')) {
      if (request.method === 'POST') {
        log = [];
        response.writeHead(200, { 'content-type': TYPES['.json'], 'access-control-allow-origin': '*' }).end('{"reset":true}');
        return;
      }
      const body = JSON.stringify(summarise(log));
      response.writeHead(200, {
        'content-type': TYPES['.json'],
        'cache-control': 'no-store',
        'access-control-allow-origin': '*',
      }).end(body);
      return;
    }
    if (url === '/__ranges/reset') {
      log = [];
      response.writeHead(200, { 'content-type': TYPES['.json'], 'access-control-allow-origin': '*' }).end('{"reset":true}');
      return;
    }

    let file = resolvePath(url);
    if (!file) {
      response.writeHead(403).end('Forbidden');
      return;
    }
    try {
      let info = await stat(file);
      if (info.isDirectory()) {
        file = join(file, 'index.html');
        info = await stat(file);
      }
      const type = TYPES[extname(file).toLowerCase()] || 'application/octet-stream';
      const tracked = file.startsWith(join(root, 'data'));
      const record = (status, bytes, range) => {
        if (!tracked) return;
        log.push({ path: file.slice(root.length), status, bytes, range: range || null, size: info.size, at: Date.now() });
      };

      if (request.method === 'HEAD') {
        response.writeHead(200, {
          'content-type': type,
          'content-length': info.size,
          'accept-ranges': 'bytes',
          'cache-control': 'no-store',
        }).end();
        record(200, 0, 'HEAD');
        return;
      }

      const wanted = request.headers.range ? parseRange(request.headers.range, info.size) : null;
      if (wanted && wanted.unsatisfiable) {
        response.writeHead(416, { 'content-range': `bytes */${info.size}` }).end();
        record(416, 0, String(request.headers.range));
        return;
      }
      if (wanted) {
        const length = wanted.end - wanted.start + 1;
        response.writeHead(206, {
          'content-type': type,
          'content-length': length,
          'content-range': `bytes ${wanted.start}-${wanted.end}/${info.size}`,
          'accept-ranges': 'bytes',
          'cache-control': 'no-store',
        });
        record(206, length, String(request.headers.range));
        createReadStream(file, { start: wanted.start, end: wanted.end }).pipe(response);
        return;
      }

      response.writeHead(200, {
        'content-type': type,
        'content-length': info.size,
        'accept-ranges': 'bytes',
        'cache-control': 'no-store',
      });
      record(200, info.size, null);
      createReadStream(file).pipe(response);
    } catch {
      response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }).end('Not found');
    }
  });

  return new Promise((done) => {
    server.listen(port, '127.0.0.1', () => done({
      server,
      port: server.address().port,
      ranges: () => summarise(log),
      reset: () => { log = []; },
    }));
  });
}

/** The tally, per file: how many requests, how many were 206, and how much came back. */
export function summarise(log) {
  const files = {};
  for (const entry of log) {
    const f = (files[entry.path] ||= { path: entry.path, size: entry.size, requests: 0, partial: 0, whole: 0, bytes: 0, ranges: [] });
    f.requests += 1;
    if (entry.status === 206) f.partial += 1;
    if (entry.status === 200) f.whole += 1;
    f.bytes += entry.bytes;
    if (entry.range) f.ranges.push(entry.range);
  }
  for (const f of Object.values(files)) {
    f.percentOfFile = f.size ? Number(((f.bytes / f.size) * 100).toFixed(1)) : null;
  }
  return { files: Object.values(files), total: log.length };
}

/* Run directly rather than imported by the verification script. */
if (process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url))) {
  const wanted = Number(process.argv[2] || 0);
  const { port } = await startServer(Number.isFinite(wanted) ? wanted : 0);
  process.stdout.write(`Serving the demo at http://localhost:${port}/\n`);
  process.stdout.write(`Range accounting at http://localhost:${port}/__ranges\n`);
  process.stdout.write('Press Control-C to stop.\n');
}
