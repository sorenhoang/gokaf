import { useState, type FormEvent } from "react";
import { usePoll } from "./usePoll";
import {
  loadCluster,
  loadGroups,
  loadProducers,
  createTopic,
  produce,
  fetchRecords,
  resetGroupOffset,
  type ClusterView,
  type FetchedRecord,
} from "./api";
import { BROKERS } from "./brokers";

type Tab = "dashboard" | "groups" | "producers" | "console";

export function App() {
  const [tab, setTab] = useState<Tab>("dashboard");
  const cluster = usePoll(loadCluster, 1000);
  const topics = cluster.data?.topics.map((t) => t.name) ?? [];

  return (
    <div className="app">
      <header>
        <h1>gokaf</h1>
        <nav>
          {(["dashboard", "groups", "producers", "console"] as Tab[]).map((t) => (
            <button key={t} className={tab === t ? "active" : ""} onClick={() => setTab(t)}>
              {t[0].toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>
        <span className="controller">
          {cluster.data
            ? cluster.data.controllerDisagreement
              ? "controller: DISAGREEMENT"
              : `controller: broker ${cluster.data.controllerId}`
            : "…"}
        </span>
      </header>

      {cluster.error && <div className="banner error">poll error: {cluster.error}</div>}
      {cluster.data?.errors.map((e, i) => (
        <div key={i} className="banner warn">
          broker unreachable: {e}
        </div>
      ))}

      {tab === "dashboard" && <Dashboard view={cluster.data} />}
      {tab === "groups" && <GroupsPanel />}
      {tab === "producers" && <ProducersPanel />}
      {tab === "console" && <Console topics={topics} onChange={cluster.refresh} />}
    </div>
  );
}

function GroupsPanel() {
  const groups = usePoll(loadGroups, 1000);
  const [reset, setReset] = useState({ group: "", topic: "", partition: 0, offset: 0 });
  const [msg, setMsg] = useState<string | null>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null);
    try {
      await resetGroupOffset(reset.group, reset.topic, reset.partition, reset.offset);
      setMsg(`reset ${reset.group} ${reset.topic}-${reset.partition} to ${reset.offset}`);
      groups.refresh();
    } catch (err) {
      setMsg(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div className="pad">
      {groups.error && <div className="banner error">{groups.error}</div>}
      <h2>Consumer groups</h2>
      {(groups.data ?? []).length === 0 && <p className="muted">no active groups</p>}
      {(groups.data ?? []).map((g) => (
        <div key={g.id} className="topic">
          <h3>
            {g.id} <span className="muted">— {g.state}, gen {g.generation_id}, protocol {g.protocol || "?"}, leader {g.leader_id || "?"}</span>
          </h3>
          {(g.members ?? []).map((m) => (
            <div key={m.id}>
              <strong>{m.id}</strong>
              {(m.assignment ?? []).length === 0 ? (
                <span className="muted"> — no assignment</span>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>topic</th>
                      <th>partition</th>
                      <th>committed</th>
                      <th>hwm</th>
                      <th>lag</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(m.assignment ?? []).map((a) => (
                      <tr key={`${a.topic}-${a.partition}`}>
                        <td>{a.topic}</td>
                        <td>{a.partition}</td>
                        <td>{a.committed_offset < 0 ? "—" : a.committed_offset}</td>
                        <td>{a.high_watermark}</td>
                        <td>{a.lag}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
      ))}

      <form className="card" onSubmit={submit} style={{ maxWidth: 360 }}>
        <h3>Reset committed offset</h3>
        <label>
          group{" "}
          <input value={reset.group} onChange={(e) => setReset({ ...reset, group: e.target.value })} required />
        </label>
        <label>
          topic{" "}
          <input value={reset.topic} onChange={(e) => setReset({ ...reset, topic: e.target.value })} required />
        </label>
        <label>
          partition{" "}
          <input
            type="number"
            min={0}
            value={reset.partition}
            onChange={(e) => setReset({ ...reset, partition: +e.target.value })}
          />
        </label>
        <label>
          offset{" "}
          <input
            type="number"
            min={0}
            value={reset.offset}
            onChange={(e) => setReset({ ...reset, offset: +e.target.value })}
          />
        </label>
        <button type="submit">Reset</button>
        {msg && <p className="msg">{msg}</p>}
      </form>
    </div>
  );
}

function ProducersPanel() {
  const producers = usePoll(loadProducers, 1000);
  return (
    <div className="pad">
      {producers.error && <div className="banner error">{producers.error}</div>}
      <h2>Idempotent producers</h2>
      {(producers.data ?? []).length === 0 && <p className="muted">no active producer ids</p>}
      {(producers.data ?? []).map((p) => (
        <div key={p.producer_id} className="topic">
          <h3>
            pid {p.producer_id} <span className="muted">— epoch {p.epoch}</span>
          </h3>
          <table>
            <thead>
              <tr>
                <th>topic</th>
                <th>partition</th>
                <th>last sequence</th>
                <th>last offset</th>
              </tr>
            </thead>
            <tbody>
              {(p.partitions ?? []).map((part) => (
                <tr key={`${part.topic}-${part.partition}`}>
                  <td>{part.topic}</td>
                  <td>{part.partition}</td>
                  <td>{part.last_sequence}</td>
                  <td>{part.last_offset}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

function Dashboard({ view }: { view: ClusterView | null }) {
  if (!view) return <p className="pad">loading…</p>;
  return (
    <div className="pad">
      <h2>Brokers</h2>
      <table>
        <thead>
          <tr>
            <th>id</th>
            <th>host</th>
            <th>port</th>
            <th>controller</th>
          </tr>
        </thead>
        <tbody>
          {view.brokers.map((b) => (
            <tr key={b.node_id}>
              <td>{b.node_id}</td>
              <td>{b.host}</td>
              <td>{b.port}</td>
              <td>{b.controller_id === b.node_id ? "yes" : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2>Topics</h2>
      {view.topics.length === 0 && (
        <p className="muted">no topics yet — create one from the Console tab</p>
      )}
      {view.topics.map((t) => (
        <div key={t.name} className="topic">
          <h3>
            {t.name}
            {view.topicDisagreements.includes(t.name) && (
              <span className="tag warn">brokers disagree</span>
            )}
          </h3>
          <table>
            <thead>
              <tr>
                <th>partition</th>
                <th>leader</th>
                <th>replicas</th>
                <th>isr</th>
                <th>start</th>
                <th>end</th>
                <th>hwm</th>
              </tr>
            </thead>
            <tbody>
              {t.partitions.map((p) => (
                <tr key={p.id}>
                  <td>{p.id}</td>
                  <td>{p.leader}</td>
                  <td>{p.replicas.join(", ")}</td>
                  <td>{p.isr.join(", ")}</td>
                  <td>{p.start_offset}</td>
                  <td>{p.end_offset}</td>
                  <td>{p.high_watermark}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

function Console({ topics, onChange }: { topics: string[]; onChange: () => void }) {
  return (
    <div className="pad console">
      <datalist id="topics">
        {topics.map((t) => (
          <option key={t} value={t} />
        ))}
      </datalist>
      <CreateTopicForm onDone={onChange} />
      <ProduceForm />
      <BrowseView />
    </div>
  );
}

function CreateTopicForm({ onDone }: { onDone: () => void }) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState(1);
  const [rf, setRf] = useState(1);
  const [msg, setMsg] = useState<string | null>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null);
    try {
      await createTopic(name, partitions, rf);
      setMsg(`created ${name}`);
      setName("");
      onDone();
    } catch (err) {
      setMsg(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <form className="card" onSubmit={submit}>
      <h3>Create topic</h3>
      <label>
        name <input value={name} onChange={(e) => setName(e.target.value)} required />
      </label>
      <label>
        partitions{" "}
        <input
          type="number"
          min={1}
          value={partitions}
          onChange={(e) => setPartitions(+e.target.value)}
        />
      </label>
      <label>
        replication factor{" "}
        <input type="number" min={1} value={rf} onChange={(e) => setRf(+e.target.value)} />
      </label>
      <button type="submit">Create</button>
      {msg && <p className="msg">{msg}</p>}
    </form>
  );
}

function ProduceForm() {
  const [topic, setTopic] = useState("");
  const [partition, setPartition] = useState(0);
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [broker, setBroker] = useState(BROKERS[0]);
  const [msg, setMsg] = useState<string | null>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null);
    try {
      const res = await produce(broker, topic, partition, key, value);
      setMsg(`base_offset ${res.base_offset}`);
      setValue("");
    } catch (err) {
      setMsg(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <form className="card" onSubmit={submit}>
      <h3>Produce</h3>
      <label>
        topic <input list="topics" value={topic} onChange={(e) => setTopic(e.target.value)} required />
      </label>
      <label>
        partition{" "}
        <input
          type="number"
          min={0}
          value={partition}
          onChange={(e) => setPartition(+e.target.value)}
        />
      </label>
      <label>
        broker{" "}
        <select value={broker} onChange={(e) => setBroker(e.target.value)}>
          {BROKERS.map((b) => (
            <option key={b} value={b}>
              {b || "this broker"}
            </option>
          ))}
        </select>
      </label>
      <label>
        key <input value={key} onChange={(e) => setKey(e.target.value)} />
      </label>
      <label>
        value <input value={value} onChange={(e) => setValue(e.target.value)} required />
      </label>
      <button type="submit">Send</button>
      {msg && <p className="msg">{msg}</p>}
    </form>
  );
}

function BrowseView() {
  const [topic, setTopic] = useState("");
  const [partition, setPartition] = useState(0);
  const [offset, setOffset] = useState(0);
  const [broker, setBroker] = useState(BROKERS[0]);
  const [records, setRecords] = useState<FetchedRecord[]>([]);
  const [hwm, setHwm] = useState<number | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const load = async (e: FormEvent) => {
    e.preventDefault();
    setMsg(null);
    try {
      const res = await fetchRecords(broker, topic, partition, offset);
      setRecords(res.records);
      setHwm(res.high_watermark);
    } catch (err) {
      setRecords([]);
      setHwm(null);
      setMsg(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <form className="card" onSubmit={load}>
      <h3>Browse</h3>
      <label>
        topic <input list="topics" value={topic} onChange={(e) => setTopic(e.target.value)} required />
      </label>
      <label>
        partition{" "}
        <input
          type="number"
          min={0}
          value={partition}
          onChange={(e) => setPartition(+e.target.value)}
        />
      </label>
      <label>
        from offset{" "}
        <input type="number" min={0} value={offset} onChange={(e) => setOffset(+e.target.value)} />
      </label>
      <label>
        broker{" "}
        <select value={broker} onChange={(e) => setBroker(e.target.value)}>
          {BROKERS.map((b) => (
            <option key={b} value={b}>
              {b || "this broker"}
            </option>
          ))}
        </select>
      </label>
      <button type="submit">Fetch</button>
      {hwm !== null && <p className="msg">high watermark: {hwm}</p>}
      {msg && <p className="msg">{msg}</p>}
      {records.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>offset</th>
              <th>key</th>
              <th>value</th>
              <th>timestamp</th>
            </tr>
          </thead>
          <tbody>
            {records.map((r) => (
              <tr key={r.offset}>
                <td>{r.offset}</td>
                <td>{r.key ?? ""}</td>
                <td>{r.value}</td>
                <td>{r.timestamp}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </form>
  );
}
