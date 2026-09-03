// Static list of broker admin API base URLs. The dashboard queries every
// entry and merges the results, flagging disagreements.
//
// The empty string means "same origin as this page" — the broker that served
// the UI. Add the other brokers' -http-addr URLs for a multi-broker view, e.g.
//   export const BROKERS = ["", "http://localhost:8081", "http://localhost:8082"];
export const BROKERS: string[] = [""];
