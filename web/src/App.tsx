import { useState, type FormEvent } from "react";
import { usePoll } from "./usePoll";
import {
  loadCluster,
  createTopic,
  produce,
  fetchRecords,
  type ClusterView,
  type FetchedRecord,
} from "./api";
import { BROKERS } from "./brokers";

type Tab = "dashboard" | "console";

export function App() {
  const [tab, setTab] = useState<Tab>("dashboard");
  const cluster = usePoll(loadCluster, 1000);
  const topics = cluster.data?.topics.map((t) => t.name) ?? [];

  return (
    <div className="app">
      <header>
        <h1>gokaf</h1>
        <nav>
          <button className={tab === "dashboard" ? "active" : ""} onClick={() => setTab("dashboard")}>
            Dashboard
          </button>
          <button className={tab === "console" ? "active" : ""} onClick={() => setTab("console")}>
            Console
          </button>
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

      {tab === "dashboard" ? (
        <Dashboard view={cluster.data} />
      ) : (
        <Console topics={topics} onChange={cluster.refresh} />
      )}
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
