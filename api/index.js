import server from '../src/server.js';

// Vercel invokes this handler per request. src/server.js automatically selects
// its serverless-safe local storage mode when the VERCEL environment is present.
export default function handler(request, response) {
  server.emit('request', request, response);
}
