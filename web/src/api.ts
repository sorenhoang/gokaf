import { BROKERS } from "./brokers";

export interface PartitionInfo {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  start_offset: number;
  end_offset: number;
  high_watermark: number;
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
}

export interface BrokerInfo {
  node_id: number;
  host: string;
  port: number;
  controller_id: number;
}

export interface FetchedRecord {
  offset: number;
  key?: string;
  value: string;
  timestamp: number;
}

async function req<T>(base: string, path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(base + path, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
  });
  const text = await res.text();
  const body = text ? JSON.parse(text) : undefined;
  if (!res.ok) {
    throw new Error(body?.error ?? `${res.status} ${res.statusText}`);
  }
  return body as T;
}

// --- multi-broker merged reads ---

export interface ClusterView {
  brokers: BrokerInfo[];
  controllerId: number | null;
  controllerDisagreement: boolean;
  topics: TopicInfo[];
  topicDisagreements: string[];
  errors: string[];
}

function partitionKey(t: string, p: PartitionInfo): string {
  return `${t}-${p.id}:${p.leader}:${p.replicas.join(",")}:${p.isr.join(",")}`;
}

export async function loadCluster(): Promise<ClusterView> {
  const results = await Promise.allSettled(
    BROKERS.map(async (base) => ({
      base,
      broker: await req<BrokerInfo>(base, "/api/v1/broker"),
      topics: await req<TopicInfo[]>(base, "/api/v1/topics"),
    })),
  );

  const brokers: BrokerInfo[] = [];
  const errors: string[] = [];
  const controllerIds = new Set<number>();
  const topicByName = new Map<string, TopicInfo>();
  const partitionSeen = new Map<string, Set<string>>();

  for (const r of results) {
    if (r.status === "rejected") {
      errors.push(String(r.reason?.message ?? r.reason));
      continue;
    }
    const { broker, topics } = r.value;
    brokers.push(broker);
    controllerIds.add(broker.controller_id);
    for (const t of topics) {
      if (!topicByName.has(t.name)) topicByName.set(t.name, t);
      let keys = partitionSeen.get(t.name);
      if (!keys) {
        keys = new Set();
        partitionSeen.set(t.name, keys);
      }
      for (const p of t.partitions) keys.add(partitionKey(t.name, p));
    }
  }

  const topicDisagreements: string[] = [];
  for (const [name, keys] of partitionSeen) {
    const byPartition = new Map<number, number>();
    for (const k of keys) {
      const pid = Number(k.split(":")[0].split("-").pop());
      byPartition.set(pid, (byPartition.get(pid) ?? 0) + 1);
    }
    if ([...byPartition.values()].some((c) => c > 1)) topicDisagreements.push(name);
  }

  brokers.sort((a, b) => a.node_id - b.node_id);
  return {
    brokers,
    controllerId: controllerIds.size === 1 ? [...controllerIds][0] : null,
    controllerDisagreement: controllerIds.size > 1,
    topics: [...topicByName.values()].sort((a, b) => a.name.localeCompare(b.name)),
    topicDisagreements,
    errors,
  };
}

// --- actions ---

const primary = BROKERS[0];

// onController runs fn against each broker until one isn't "not controller"
// (HTTP 421) — CreateTopics/DeleteTopics only succeed on the elected controller.
async function onController<T>(fn: (base: string) => Promise<T>): Promise<T> {
  let lastErr: unknown;
  for (const base of BROKERS) {
    try {
      return await fn(base);
    } catch (e) {
      lastErr = e;
      const msg = e instanceof Error ? e.message : String(e);
      if (!/not controller/i.test(msg)) throw e;
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

export function createTopic(name: string, partitions: number, replicationFactor: number) {
  return onController((base) =>
    req<{ name: string }>(base, "/api/v1/topics", {
      method: "POST",
      body: JSON.stringify({ name, partitions, replication_factor: replicationFactor }),
    }),
  );
}

export function deleteTopic(name: string) {
  return onController((base) =>
    req<void>(base, `/api/v1/topics/${encodeURIComponent(name)}`, { method: "DELETE" }),
  );
}

export function produce(base: string, topic: string, partition: number, key: string, value: string) {
  return req<{ base_offset: number }>(base, "/api/v1/produce", {
    method: "POST",
    body: JSON.stringify({ topic, partition, key, value }),
  });
}

export function fetchRecords(base: string, topic: string, partition: number, offset: number) {
  return req<{ high_watermark: number; records: FetchedRecord[] }>(base, "/api/v1/fetch", {
    method: "POST",
    body: JSON.stringify({ topic, partition, offset }),
  });
}

// --- groups + producers ---

export interface GroupPartition {
  topic: string;
  partition: number;
  committed_offset: number;
  high_watermark: number;
  lag: number;
}
export interface GroupMember {
  id: string;
  assignment: GroupPartition[];
}
export interface Group {
  id: string;
  state: string;
  generation_id: number;
  leader_id: string;
  protocol: string;
  members: GroupMember[];
}
export interface ProducerPartition {
  topic: string;
  partition: number;
  last_sequence: number;
  last_offset: number;
}
export interface Producer {
  producer_id: number;
  epoch: number;
  partitions: ProducerPartition[];
}

export function loadGroups() {
  return req<Group[]>(primary, "/api/v1/groups");
}

export function loadProducers() {
  return req<Producer[]>(primary, "/api/v1/producers");
}

export function resetGroupOffset(group: string, topic: string, partition: number, offset: number) {
  return req<unknown>(primary, `/api/v1/groups/${encodeURIComponent(group)}/reset-offset`, {
    method: "POST",
    body: JSON.stringify({ topic, partition, offset }),
  });
}

// --- cluster + chaos ---

export interface PeerReachability {
  id: number;
  alive: boolean;
}
export interface ClusterInfoRaw {
  brokers: BrokerInfo[];
  controller_id: number;
  self: number;
  peers: PeerReachability[];
}
export interface Faults {
  slow_follower_delay_ms: number;
  drop_pings: boolean;
  paused: boolean;
}

export interface PerBrokerCluster {
  base: string;
  self: number | null;
  controllerId: number | null;
  peers: PeerReachability[];
  faults: Faults | null;
  error: string | null;
}

export async function loadClusterPanel(): Promise<PerBrokerCluster[]> {
  return Promise.all(
    BROKERS.map(async (base): Promise<PerBrokerCluster> => {
      try {
        const info = await req<ClusterInfoRaw>(base, "/api/v1/cluster");
        let faults: Faults | null = null;
        try {
          faults = await req<Faults>(base, "/api/v1/faults");
        } catch {
          /* faults optional */
        }
        return { base, self: info.self, controllerId: info.controller_id, peers: info.peers, faults, error: null };
      } catch (e) {
        return {
          base,
          self: null,
          controllerId: null,
          peers: [],
          faults: null,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    }),
  );
}

export function setFaults(base: string, body: Partial<Faults>) {
  return req<Faults>(base, "/api/v1/faults", { method: "POST", body: JSON.stringify(body) });
}

export function shutdownBroker(base: string) {
  return req<unknown>(base, "/api/v1/shutdown", { method: "POST" });
}
