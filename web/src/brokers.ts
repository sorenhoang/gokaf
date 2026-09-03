// Static list of broker admin API base URLs. The dashboard queries every
// entry and merges the results, flagging disagreements.
//
// "" means "same origin as this page" (the broker that served the UI). The
// other two entries are the default -http-addr ports for a local 3-broker
// cluster; drop them to a single "" for a one-broker setup.
export const BROKERS: string[] = ["", "http://localhost:8081", "http://localhost:8082"];
