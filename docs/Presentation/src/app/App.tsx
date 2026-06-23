import { useState, useEffect, useCallback } from "react";
import { ChevronLeft, ChevronRight, Database, Server, HardDrive, Network, Shield, Terminal, GitBranch, Layers } from "lucide-react";

const slides = [
  { id: 1, component: "SlideObjective" },
  { id: 2, component: "SlideArchitecture" },
  { id: 3, component: "SlideDataHandling" },
  { id: 4, component: "SlideCreate" },
  { id: 5, component: "SlideReadDelete" },
  { id: 6, component: "SlideFaultTolerance" },
  { id: 7, component: "SlideDeployment" },
  { id: 8, component: "SlideConclusion" },
];

function SlideLabel({ children }: { children: React.ReactNode }) {
  return (
    <span className="font-mono text-xs tracking-[0.2em] uppercase text-primary/70 border border-primary/20 px-2 py-0.5 rounded-sm">
      {children}
    </span>
  );
}

function Badge({ children, color = "blue" }: { children: React.ReactNode; color?: "blue" | "cyan" | "green" | "orange" | "purple" | "red" }) {
  const colors = {
    blue: "bg-blue-500/10 text-blue-300 border-blue-500/20",
    cyan: "bg-cyan-500/10 text-cyan-300 border-cyan-500/20",
    green: "bg-emerald-500/10 text-emerald-300 border-emerald-500/20",
    orange: "bg-orange-500/10 text-orange-300 border-orange-500/20",
    purple: "bg-purple-500/10 text-purple-300 border-purple-500/20",
    red: "bg-red-500/10 text-red-300 border-red-500/20",
  };
  return (
    <span className={`font-mono text-xs px-2 py-0.5 rounded border ${colors[color]}`}>
      {children}
    </span>
  );
}

function SlideObjective() {
  return (
    <div className="h-full flex flex-col items-center justify-center relative overflow-hidden">
      {/* Background grid */}
      <div className="absolute inset-0 opacity-[0.03]" style={{
        backgroundImage: "linear-gradient(rgba(56,189,248,1) 1px, transparent 1px), linear-gradient(90deg, rgba(56,189,248,1) 1px, transparent 1px)",
        backgroundSize: "60px 60px"
      }} />
      {/* Glow */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[400px] rounded-full blur-[120px] opacity-10"
        style={{ background: "radial-gradient(ellipse, #38bdf8 0%, #06b6d4 50%, transparent 70%)" }} />

      <div className="relative z-10 text-center max-w-4xl px-8">
        <SlideLabel>01 / Project Objective</SlideLabel>
        <h1 className="font-display text-6xl font-bold mt-6 mb-3 leading-none tracking-tight text-foreground">
          Distributed
          <span className="block text-transparent bg-clip-text" style={{ backgroundImage: "linear-gradient(90deg, #38bdf8, #06b6d4)" }}>
            File System
          </span>
        </h1>
        <p className="font-body text-muted-foreground text-base mb-10 max-w-xl mx-auto leading-relaxed">
          Inspired by the Google File System architecture. A client-based system for text-oriented files
          that hides physical distribution behind a clean file interface.
        </p>

        <div className="grid grid-cols-3 gap-4 max-w-2xl mx-auto">
          {[
            { icon: <Layers size={20} />, label: "Sharding", desc: "Fixed-size chunks distributed across machines" },
            { icon: <GitBranch size={20} />, label: "Replication", desc: "Each chunk stored on 3 distinct servers" },
            { icon: <Database size={20} />, label: "Separation", desc: "Metadata layer independent from content" },
          ].map((item) => (
            <div key={item.label} className="bg-card border border-border rounded p-4 text-left hover:border-primary/30 transition-colors">
              <div className="text-primary mb-2">{item.icon}</div>
              <div className="font-display font-semibold text-sm text-foreground mb-1">{item.label}</div>
              <div className="font-body text-xs text-muted-foreground leading-relaxed">{item.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function NodeBox({ label, sublabel, color = "blue", icon }: { label: string; sublabel?: string; color?: string; icon: React.ReactNode }) {
  const colors: Record<string, string> = {
    blue: "border-blue-500/40 bg-blue-500/5 text-blue-300",
    cyan: "border-cyan-500/40 bg-cyan-500/5 text-cyan-300",
    purple: "border-purple-500/40 bg-purple-500/5 text-purple-300",
  };
  return (
    <div className={`border rounded p-3 flex flex-col items-center gap-1.5 ${colors[color]}`}>
      <div className="opacity-80">{icon}</div>
      <div className="font-display font-semibold text-xs text-center leading-tight">{label}</div>
      {sublabel && <div className="font-mono text-[10px] opacity-50">{sublabel}</div>}
    </div>
  );
}

function SlideArchitecture() {
  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>02 / System Architecture</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Three-Layer Architecture</h2>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        {/* Diagram */}
        <div className="flex-1 flex flex-col justify-center">
          <div className="relative">
            {/* Client */}
            <div className="flex justify-center mb-4">
              <div className="border border-cyan-500/40 bg-cyan-500/5 rounded px-6 py-3 flex items-center gap-3">
                <Terminal size={18} className="text-cyan-300" />
                <div>
                  <div className="font-display font-semibold text-sm text-cyan-300">Client</div>
                  <div className="font-mono text-[10px] text-muted-foreground">File-level interface · gRPC transfers</div>
                </div>
              </div>
            </div>

            {/* Arrow down to naming */}
            <div className="flex justify-center mb-1">
              <div className="flex flex-col items-center">
                <div className="w-px h-4 bg-primary/30" />
                <div className="text-[9px] font-mono text-muted-foreground px-2 py-0.5 border border-border rounded-sm bg-card">metadata / placement</div>
                <div className="w-px h-4 bg-primary/30" />
              </div>
            </div>

            {/* Naming Server */}
            <div className="flex justify-center mb-1">
              <div className="border border-blue-500/40 bg-blue-500/5 rounded px-6 py-3 flex items-center gap-3">
                <Database size={18} className="text-blue-300" />
                <div>
                  <div className="font-display font-semibold text-sm text-blue-300">Naming Server</div>
                  <div className="font-mono text-[10px] text-muted-foreground">SQLite metadata · single authority · heartbeat monitor</div>
                </div>
              </div>
            </div>

            {/* Arrow down to storage */}
            <div className="flex justify-center mb-1">
              <div className="flex flex-col items-center">
                <div className="w-px h-4 bg-primary/30" />
                <div className="text-[9px] font-mono text-muted-foreground px-2 py-0.5 border border-border rounded-sm bg-card">chunk data · direct transfer</div>
                <div className="w-px h-4 bg-primary/30" />
              </div>
            </div>

            {/* Storage Servers */}
            <div className="grid grid-cols-4 gap-2">
              {["S1", "S2", "S3", "S4"].map((s) => (
                <NodeBox key={s} label={`Storage ${s}`} sublabel="disk · heartbeat" color="purple" icon={<HardDrive size={16} />} />
              ))}
            </div>
          </div>
        </div>

        {/* Component descriptions */}
        <div className="w-72 flex flex-col gap-3">
          {[
            {
              icon: <Terminal size={16} className="text-cyan-300" />,
              title: "Client",
              color: "border-cyan-500/20",
              points: [
                "Requests metadata from naming server",
                "Transfers chunk data directly to/from storage",
                "Provides create, read, delete, size, ls"
              ]
            },
            {
              icon: <Database size={16} className="text-blue-300" />,
              title: "Naming Server",
              color: "border-blue-500/20",
              points: [
                "Single metadata authority",
                "Tracks files, chunks, replica locations",
                "SQLite persistence — survives restart",
                "Never stores chunk content"
              ]
            },
            {
              icon: <HardDrive size={16} className="text-purple-300" />,
              title: "Storage Servers (×4)",
              color: "border-purple-500/20",
              points: [
                "Store chunk bytes as local files",
                "Each holds a subset of total chunks",
                "Register + send heartbeats every 5s"
              ]
            },
          ].map((comp) => (
            <div key={comp.title} className={`bg-card border ${comp.color} rounded p-4`}>
              <div className="flex items-center gap-2 mb-2">
                {comp.icon}
                <span className="font-display font-semibold text-sm text-foreground">{comp.title}</span>
              </div>
              <ul className="space-y-1">
                {comp.points.map((p) => (
                  <li key={p} className="font-body text-xs text-muted-foreground flex gap-2">
                    <span className="text-primary/40 mt-0.5">›</span>
                    <span>{p}</span>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SlideDataHandling() {
  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>03 / Data Handling</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Chunks, UUIDs & Replication</h2>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        {/* Visual chunk diagram */}
        <div className="flex-1 flex flex-col justify-center gap-6">
          {/* File → Chunks */}
          <div>
            <div className="font-mono text-xs text-muted-foreground mb-3 uppercase tracking-wider">File → Chunk partitioning</div>
            <div className="flex items-center gap-2">
              <div className="border border-border bg-card rounded px-4 py-2 font-mono text-sm text-foreground">
                document.txt <span className="text-muted-foreground">(2.3 KB)</span>
              </div>
              <div className="text-muted-foreground font-mono">→</div>
              {["Chunk 0", "Chunk 1", "Chunk 2"].map((c, i) => (
                <div key={c} className={`border rounded px-3 py-2 font-mono text-xs text-center ${i < 2 ? "border-primary/30 bg-primary/5 text-primary" : "border-border bg-card text-muted-foreground"}`}>
                  <div>{c}</div>
                  <div className="text-[10px] opacity-60">{i < 2 ? "1 024 B" : "279 B"}</div>
                </div>
              ))}
            </div>
          </div>

          {/* UUID */}
          <div>
            <div className="font-mono text-xs text-muted-foreground mb-3 uppercase tracking-wider">UUID chunk identifier</div>
            <div className="bg-card border border-border rounded p-3 font-mono text-sm text-primary/80">
              <span className="text-muted-foreground">chunk_id: </span>
              a3f8b2c194e04d71b3a21f0c8e5d7a29
            </div>
          </div>

          {/* Replication */}
          <div>
            <div className="font-mono text-xs text-muted-foreground mb-3 uppercase tracking-wider">Replication factor 3 — round-robin placement</div>
            <div className="grid grid-cols-4 gap-2">
              {[
                { s: "S1", has: true },
                { s: "S2", has: true },
                { s: "S3", has: true },
                { s: "S4", has: false },
              ].map(({ s, has }) => (
                <div key={s} className={`border rounded p-3 text-center ${has ? "border-emerald-500/30 bg-emerald-500/5" : "border-border bg-card"}`}>
                  <HardDrive size={16} className={`mx-auto mb-1 ${has ? "text-emerald-400" : "text-muted-foreground"}`} />
                  <div className={`font-mono text-xs ${has ? "text-emerald-300" : "text-muted-foreground"}`}>{s}</div>
                  <div className="font-mono text-[10px] mt-0.5" style={{ color: has ? "#6ee7b7" : "#6b85a3" }}>
                    {has ? "replica" : "—"}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Spec table */}
        <div className="w-72 flex flex-col gap-3">
          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-3">Configuration</div>
            <div className="space-y-2">
              {[
                ["Workload", "Text-oriented files"],
                ["Chunk size", "1 024 bytes fixed"],
                ["Last chunk", "May be smaller"],
                ["Identifier", "UUID per chunk"],
                ["Replication", "Factor 3"],
                ["Placement", "Round-robin (live)"],
                ["SQLite", "Metadata only"],
                ["Content", "Disk files on servers"],
              ].map(([k, v]) => (
                <div key={k} className="flex justify-between items-start gap-2">
                  <span className="font-mono text-xs text-muted-foreground">{k}</span>
                  <span className="font-body text-xs text-foreground text-right">{v}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-2">Storage separation</div>
            <p className="font-body text-xs text-muted-foreground leading-relaxed">
              Only metadata is in SQLite. Chunk bytes live as regular files under each storage server's <span className="font-mono text-primary/70">/data</span> directory — satisfying the required metadata/content separation.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function StageStep({ n, title, desc, active }: { n: number; title: string; desc: string; active?: boolean }) {
  return (
    <div className={`border rounded p-4 transition-colors ${active ? "border-primary/50 bg-primary/5" : "border-border bg-card"}`}>
      <div className="flex items-center gap-3 mb-2">
        <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-mono font-bold ${active ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground"}`}>
          {n}
        </div>
        <span className={`font-display font-semibold text-sm ${active ? "text-foreground" : "text-muted-foreground"}`}>{title}</span>
      </div>
      <p className="font-body text-xs text-muted-foreground leading-relaxed pl-9">{desc}</p>
    </div>
  );
}

function SlideCreate() {
  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>04 / Create Operation</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Four-Stage Write Pipeline</h2>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        <div className="flex-1 flex flex-col justify-center gap-3">
          <StageStep n={1} title="Split" desc="Client splits the input into 1 KB chunks and calculates total file size." />
          <StageStep n={2} title="Request placement" desc="Client calls CreateFile on naming server. Server checks live capacity, assigns a UUID to every chunk, and returns a placement plan with 3 replica locations per chunk." active />
          <StageStep n={3} title="Upload replicas" desc="Client uploads each chunk directly to all 3 assigned storage servers. Each server writes through a temp file then performs an atomic rename." active />
          <StageStep n={4} title="Commit" desc="Client calls CommitFile. Naming server transitions state from pending → committed, making the file visible to reads. Interrupted uploads leave no partial file." />
        </div>

        <div className="w-72 flex flex-col gap-3">
          {/* State machine */}
          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-3">Metadata state machine</div>
            <div className="flex items-center gap-3">
              <div className="border border-orange-500/30 bg-orange-500/5 rounded px-3 py-2 text-center">
                <div className="font-mono text-xs text-orange-300 font-medium">PENDING</div>
                <div className="font-mono text-[10px] text-muted-foreground mt-0.5">not readable</div>
              </div>
              <div className="flex-1 text-center">
                <div className="font-mono text-[10px] text-muted-foreground">CommitFile</div>
                <div className="w-full h-px bg-primary/30 mt-1" />
              </div>
              <div className="border border-emerald-500/30 bg-emerald-500/5 rounded px-3 py-2 text-center">
                <div className="font-mono text-xs text-emerald-300 font-medium">COMMITTED</div>
                <div className="font-mono text-[10px] text-muted-foreground mt-0.5">readable</div>
              </div>
            </div>
          </div>

          {/* Sequence note */}
          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-2">Sequence</div>
            <div className="space-y-1.5 font-mono text-xs">
              {[
                { from: "client", to: "naming", msg: "CreateFile(name, size)" },
                { from: "naming", to: "client", msg: "placement plan" },
                { from: "client", to: "S1/S2/S3", msg: "StoreChunk(uuid, data)" },
                { from: "S*", to: "client", msg: "ack" },
                { from: "client", to: "naming", msg: "CommitFile(name)" },
              ].map((s, i) => (
                <div key={i} className="text-[11px]">
                  <span className="text-primary/70">{s.from}</span>
                  <span className="text-muted-foreground"> → </span>
                  <span className="text-cyan-300/70">{s.to}</span>
                  <span className="text-muted-foreground">: </span>
                  <span className="text-foreground/80">{s.msg}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-2">Atomicity guarantee</div>
            <p className="font-body text-xs text-muted-foreground leading-relaxed">
              Storage servers write to a temp file before renaming atomically. No partial chunk is ever exposed.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function SlideReadDelete() {
  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>05 / Read, Delete & Size</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Client-Side Operations</h2>
      </div>

      <div className="flex-1 grid grid-cols-3 gap-4 min-h-0">
        {/* Read */}
        <div className="bg-card border border-border rounded p-5 flex flex-col">
          <div className="flex items-center gap-2 mb-4">
            <Network size={18} className="text-primary" />
            <span className="font-display font-semibold text-foreground">Read</span>
          </div>
          <ol className="space-y-3 flex-1">
            {[
              "Client requests chunk layout + replica addresses from naming server",
              "Downloads each chunk directly from a storage server",
              "Concatenates chunks in index order to reconstruct file",
            ].map((step, i) => (
              <li key={i} className="flex gap-2">
                <span className="font-mono text-xs text-primary/50 mt-0.5 shrink-0">{String(i + 1).padStart(2, "0")}</span>
                <span className="font-body text-xs text-muted-foreground leading-relaxed">{step}</span>
              </li>
            ))}
          </ol>
          <div className="mt-4 border-t border-border pt-4">
            <div className="font-mono text-xs text-muted-foreground mb-2">Replica fallback</div>
            <div className="space-y-1.5">
              {[
                { s: "S1", status: "down", label: "try next" },
                { s: "S2", status: "ok", label: "success" },
                { s: "S3", status: "skip", label: "not needed" },
              ].map(({ s, status, label }) => (
                <div key={s} className="flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ${status === "ok" ? "bg-emerald-400" : status === "down" ? "bg-red-400" : "bg-muted"}`} />
                  <span className="font-mono text-xs text-foreground">{s}</span>
                  <span className={`font-mono text-[10px] ml-auto ${status === "ok" ? "text-emerald-300" : status === "down" ? "text-red-300" : "text-muted-foreground"}`}>{label}</span>
                </div>
              ))}
            </div>
            <p className="font-body text-[11px] text-muted-foreground mt-2 leading-relaxed">
              Naming server prioritizes live replicas in its response.
            </p>
          </div>
        </div>

        {/* Delete */}
        <div className="bg-card border border-border rounded p-5 flex flex-col">
          <div className="flex items-center gap-2 mb-4">
            <Server size={18} className="text-red-400" />
            <span className="font-display font-semibold text-foreground">Delete</span>
          </div>
          <ol className="space-y-3 flex-1">
            {[
              "Naming server requests deletion of every known chunk replica",
              "Then removes the file metadata entry from SQLite",
            ].map((step, i) => (
              <li key={i} className="flex gap-2">
                <span className="font-mono text-xs text-red-400/50 mt-0.5 shrink-0">{String(i + 1).padStart(2, "0")}</span>
                <span className="font-body text-xs text-muted-foreground leading-relaxed">{step}</span>
              </li>
            ))}
          </ol>
          <div className="mt-4 border-t border-border pt-4 space-y-3">
            <div>
              <Badge color="orange">Idempotent</Badge>
              <p className="font-body text-[11px] text-muted-foreground mt-1.5 leading-relaxed">
                Chunk deletion calls are safe to repeat.
              </p>
            </div>
            <div>
              <Badge color="red">Orphan chunks</Badge>
              <p className="font-body text-[11px] text-muted-foreground mt-1.5 leading-relaxed">
                If a server is unavailable, its copy becomes an unreferenced orphan — unreachable through the file system but not yet removed. Garbage collection is future work.
              </p>
            </div>
          </div>
        </div>

        {/* Size + List */}
        <div className="bg-card border border-border rounded p-5 flex flex-col">
          <div className="flex items-center gap-2 mb-4">
            <Database size={18} className="text-cyan-400" />
            <span className="font-display font-semibold text-foreground">Size & List</span>
          </div>
          <div className="flex-1 space-y-4">
            <div>
              <div className="font-mono text-xs text-muted-foreground mb-2">size(filename)</div>
              <div className="bg-background border border-border rounded p-3">
                <p className="font-body text-xs text-muted-foreground leading-relaxed">
                  Answered exclusively from metadata. Returns total byte count and number of chunks — <span className="text-foreground">no chunk data is transferred</span>.
                </p>
                <div className="mt-2 font-mono text-xs">
                  <span className="text-muted-foreground">→ </span>
                  <span className="text-primary">{"{ bytes: 2359, chunks: 3 }"}</span>
                </div>
              </div>
            </div>

            <div>
              <div className="font-mono text-xs text-muted-foreground mb-2">ls()</div>
              <div className="bg-background border border-border rounded p-3">
                <p className="font-body text-xs text-muted-foreground leading-relaxed mb-2">
                  Convenience operation. Lists all committed files in the namespace.
                </p>
                <div className="font-mono text-xs space-y-0.5">
                  {["document.txt", "report.md", "notes.txt"].map((f) => (
                    <div key={f} className="text-foreground/70">{f}</div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SlideFaultTolerance() {
  const scenarios = [
    {
      label: "1 server down",
      status: "ok",
      desc: "Reads continue through other replicas. 3 servers alive — new writes can still achieve RF=3.",
      color: "emerald"
    },
    {
      label: "2 servers down",
      status: "warn",
      desc: "Every chunk has ≥1 surviving replica. Reads continue. New writes rejected — RF=3 cannot be satisfied.",
      color: "orange"
    },
    {
      label: "All 3 replicas lost",
      status: "error",
      desc: "Chunk unreadable. Recoverable if server returns with disk intact. Permanent disk destruction → data loss.",
      color: "red"
    },
    {
      label: "Naming server down",
      status: "warn",
      desc: "Clients cannot locate chunks or perform operations. Process restart is recoverable — SQLite persists metadata.",
      color: "orange"
    },
  ];

  const healSteps = [
    "Heartbeat every 5s; server marked unavailable after 15s silence",
    "Every 5s: naming server scans committed chunks for under-replication",
    "Chunk with < 3 live replicas + enough live capacity → trigger repair",
    "Choose surviving source + target server (doesn't hold chunk)",
    "Target retrieves bytes via ReplicateChunk RPC from source",
    "Metadata updated only after copy succeeds — RF restored silently",
  ];

  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-5">
        <SlideLabel>06 / Fault Tolerance & Self-Healing</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Resilience Model</h2>
      </div>

      <div className="flex-1 flex gap-5 min-h-0">
        {/* Failure scenarios */}
        <div className="flex-1 flex flex-col gap-2">
          <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-1">Failure scenarios (RF=3, 4 servers)</div>
          {scenarios.map((s) => {
            const icons = { ok: "●", warn: "◐", error: "○" };
            const textColor = { ok: "text-emerald-400", warn: "text-orange-400", error: "text-red-400" };
            const borderColor = { ok: "border-emerald-500/20", warn: "border-orange-500/20", error: "border-red-500/20" };
            const bgColor = { ok: "bg-emerald-500/5", warn: "bg-orange-500/5", error: "bg-red-500/5" };
            return (
              <div key={s.label} className={`border ${borderColor[s.status as keyof typeof borderColor]} ${bgColor[s.status as keyof typeof bgColor]} rounded p-3 flex gap-3`}>
                <span className={`font-mono text-sm ${textColor[s.status as keyof typeof textColor]} shrink-0 mt-0.5`}>{icons[s.status as keyof typeof icons]}</span>
                <div>
                  <div className="font-display font-semibold text-sm text-foreground">{s.label}</div>
                  <div className="font-body text-xs text-muted-foreground mt-0.5 leading-relaxed">{s.desc}</div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Self-healing */}
        <div className="w-80 flex flex-col">
          <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-2">Automatic self-healing</div>
          <div className="bg-card border border-primary/20 rounded p-4 flex-1">
            <div className="flex items-center gap-2 mb-4">
              <Shield size={16} className="text-primary" />
              <span className="font-display font-semibold text-sm text-foreground">Re-replication sequence</span>
            </div>
            <div className="space-y-2">
              {healSteps.map((step, i) => (
                <div key={i} className="flex gap-2">
                  <div className="w-5 h-5 rounded-full bg-primary/10 border border-primary/20 flex items-center justify-center shrink-0 mt-0.5">
                    <span className="font-mono text-[9px] text-primary">{i + 1}</span>
                  </div>
                  <span className="font-body text-[11px] text-muted-foreground leading-relaxed">{step}</span>
                </div>
              ))}
            </div>
            <div className="mt-4 pt-3 border-t border-border">
              <p className="font-body text-xs text-primary/80 leading-relaxed">
                RF restored automatically — zero client intervention required.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SlideDeployment() {
  const tests = [
    { id: "T1", desc: "Create + read correctness (multi-chunk file)" },
    { id: "T2", desc: "Metadata-only size query" },
    { id: "T3", desc: "Read after one storage-server failure" },
    { id: "T4", desc: "Replication factor restoration via self-healing" },
    { id: "T5", desc: "Deletion of file and replicas" },
    { id: "T6", desc: "Reject create when insufficient servers alive" },
  ];

  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>07 / Deployment, Metrics & Verification</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Docker, Observability & Tests</h2>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        {/* Docker topology */}
        <div className="flex-1 flex flex-col gap-4">
          <div className="bg-card border border-border rounded p-4">
            <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-3">Docker Compose topology</div>
            <div className="space-y-2">
              {[
                { name: "naming", port: "50051 host", role: "Metadata authority", color: "text-blue-300" },
                { name: "storage1", port: "50061 int.", role: "Replica store", color: "text-purple-300" },
                { name: "storage2", port: "50061 int.", role: "Replica store", color: "text-purple-300" },
                { name: "storage3", port: "50061 int.", role: "Replica store", color: "text-purple-300" },
                { name: "storage4", port: "50061 int.", role: "Replica store", color: "text-purple-300" },
                { name: "client", port: "", role: "Helper container", color: "text-cyan-300" },
              ].map((c) => (
                <div key={c.name} className="flex items-center gap-3 py-1.5 border-b border-border/50 last:border-0">
                  <div className={`font-mono text-xs font-medium ${c.color} w-32`}>{c.name}</div>
                  <div className="font-mono text-xs text-muted-foreground w-24">{c.port}</div>
                  <div className="font-body text-xs text-muted-foreground">{c.role}</div>
                </div>
              ))}
            </div>
            <p className="font-body text-xs text-muted-foreground mt-3">
              All containers share one internal Docker network. Only the naming-server port is exposed to host clients.
            </p>
          </div>

          <div className="bg-card border border-border rounded p-4">
            <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-3">Client commands</div>
            <div className="space-y-1.5">
              {[
                ["create", "<local> [remote]", "Upload and shard a file"],
                ["read", "<remote> [out]", "Download and reconstruct"],
                ["delete", "<remote>", "Remove replicas + metadata"],
                ["size", "<remote>", "Metadata-only byte count"],
                ["ls", "", "List committed files"],
              ].map(([cmd, arg, desc]) => (
                <div key={cmd} className="flex items-center gap-2">
                  <span className="font-mono text-xs text-primary">$ python -m gfs.client</span>
                  <span className="font-mono text-xs text-foreground">{cmd}</span>
                  <span className="font-mono text-xs text-cyan-300/60">{arg}</span>
                  <span className="font-body text-[11px] text-muted-foreground ml-auto">{desc}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Tests */}
        <div className="w-72 flex flex-col">
          <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-2">Integration test suite (6 scenarios)</div>
          <div className="flex-1 space-y-2">
            {tests.map((t) => (
              <div key={t.id} className="bg-card border border-border rounded p-3 flex gap-3">
                <div className="flex items-start gap-1.5 shrink-0 mt-0.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 mt-1" />
                  <span className="font-mono text-xs text-emerald-300">{t.id}</span>
                </div>
                <span className="font-body text-xs text-muted-foreground leading-relaxed">{t.desc}</span>
              </div>
            ))}
          </div>
          <div className="mt-3 bg-card border border-cyan-500/20 rounded p-3">
            <div className="font-mono text-xs text-cyan-300 mb-1">Monitoring & simulation</div>
            <p className="font-body text-[11px] text-muted-foreground leading-relaxed">
              Prometheus and Grafana expose live servers, files, bytes, RPC latency/throughput,
              storage usage, and healing signals. The production simulator drives configurable
              users, file sizes, and write/read/delete ratios.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function SlideConclusion() {
  return (
    <div className="h-full flex flex-col px-12 py-10 overflow-hidden">
      <div className="mb-6">
        <SlideLabel>08 / Design Decisions & Conclusion</SlideLabel>
        <h2 className="font-display text-4xl font-bold mt-3 text-foreground">Trade-offs & Summary</h2>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        {/* Decisions + trade-offs */}
        <div className="flex-1 flex flex-col gap-4">
          <div className="bg-card border border-border rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-3">Principal design decisions</div>
            <div className="space-y-2">
              {[
                ["Single naming server", "Simple, consistent metadata without coordination overhead"],
                ["SQLite persistence", "Durable metadata — naming server survives process restart"],
                ["Direct client↔storage", "Bulk data bypasses naming server — no bottleneck"],
                ["Synchronous replication", "All replicas confirmed before commit — strong durability"],
                ["Fixed 1 KB chunks", "Required by specification; enables uniform placement"],
              ].map(([decision, reason]) => (
                <div key={decision as string} className="flex gap-3">
                  <span className="text-primary/40 mt-0.5 shrink-0">›</span>
                  <div>
                    <span className="font-display font-semibold text-xs text-foreground">{decision}</span>
                    <span className="font-body text-xs text-muted-foreground"> — {reason}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-card border border-orange-500/15 rounded p-4">
            <div className="font-display font-semibold text-sm text-foreground mb-3">Known limitations</div>
            <div className="grid grid-cols-2 gap-2">
              {[
                "Naming server is single point of availability",
                "Synchronous replication increases write latency",
                "No garbage collection for orphaned chunks",
                "No chunk checksums or strict UTF-8 validation",
              ].map((lim) => (
                <div key={lim} className="flex gap-2">
                  <span className="text-orange-400/60 mt-0.5 shrink-0">!</span>
                  <span className="font-body text-xs text-muted-foreground leading-relaxed">{lim}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Conclusion */}
        <div className="w-72 flex flex-col gap-4">
          <div className="bg-card border border-primary/20 rounded p-5 flex-1 flex flex-col">
            <div className="flex items-center gap-2 mb-4">
              <Shield size={16} className="text-primary" />
              <span className="font-display font-semibold text-sm text-foreground">Conclusion</span>
            </div>
            <div className="space-y-3 flex-1">
              {[
                { icon: "✓", text: "Files partitioned into fixed-size chunks" },
                { icon: "✓", text: "Each chunk replicated across distinct storage servers" },
                { icon: "✓", text: "Metadata separated from content" },
                { icon: "✓", text: "Client hides physical distribution" },
                { icon: "✓", text: "Create, read, delete, size, and ls implemented" },
                { icon: "✓", text: "Automatic self-healing restores lost replicas" },
              ].map((item) => (
                <div key={item.text} className="flex gap-2">
                  <span className="text-emerald-400 font-mono text-sm shrink-0">{item.icon}</span>
                  <span className="font-body text-xs text-muted-foreground leading-relaxed">{item.text}</span>
                </div>
              ))}
            </div>
            <div className="mt-4 pt-4 border-t border-border">
              <p className="font-body text-xs text-primary/80 leading-relaxed">
                The system demonstrates both immediate failover through replication and subsequent recovery through automatic re-replication.
              </p>
            </div>
          </div>

          <div className="bg-card border border-border rounded p-4">
            <div className="font-mono text-xs text-muted-foreground uppercase tracking-wider mb-2">Final capabilities</div>
            <div className="space-y-1">
              {[
                ["Sharding", true],
                ["Replication (RF=3)", true],
                ["Metadata separation", true],
                ["Fault tolerance", true],
                ["Self-healing", true],
                ["Docker deployment", true],
                ["Observability", true],
                ["Integration tests", true],
              ].map(([req, done]) => (
                <div key={req as string} className="flex items-center justify-between">
                  <span className="font-body text-xs text-muted-foreground">{req}</span>
                  <span className={`font-mono text-xs ${done ? "text-emerald-300" : "text-red-300"}`}>{done ? "PASS" : "FAIL"}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const slideComponents: Record<string, React.FC> = {
  SlideObjective,
  SlideArchitecture,
  SlideDataHandling,
  SlideCreate,
  SlideReadDelete,
  SlideFaultTolerance,
  SlideDeployment,
  SlideConclusion,
};

export default function App() {
  const [current, setCurrent] = useState(0);
  const total = slides.length;

  const go = useCallback((dir: number) => {
    setCurrent((c) => Math.max(0, Math.min(total - 1, c + dir)));
  }, [total]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "ArrowRight" || e.key === "ArrowDown") go(1);
      if (e.key === "ArrowLeft" || e.key === "ArrowUp") go(-1);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [go]);

  const SlideComp = slideComponents[slides[current].component];

  return (
    <div
      className="size-full bg-background flex flex-col"
      style={{ fontFamily: "'Inter', sans-serif" }}
    >
      <style>{`
        * { --font-display: 'Rajdhani', sans-serif; --font-body: 'Inter', sans-serif; --font-mono: 'JetBrains Mono', monospace; }
        .font-display { font-family: var(--font-display); }
        .font-body { font-family: var(--font-body); }
        .font-mono { font-family: var(--font-mono); }
        ::-webkit-scrollbar { display: none; }
      `}</style>

      {/* Header */}
      <div className="flex items-center justify-between px-8 py-3 border-b border-border/50">
        <div className="flex items-center gap-3">
          <div className="w-2 h-2 rounded-full bg-primary animate-pulse" />
          <span className="font-mono text-xs text-muted-foreground tracking-wider uppercase">Distributed File System</span>
        </div>
        <div className="font-mono text-xs text-muted-foreground">
          <span className="text-foreground">{String(current + 1).padStart(2, "0")}</span>
          <span className="mx-1">/</span>
          <span>{String(total).padStart(2, "0")}</span>
        </div>
      </div>

      {/* Slide area */}
      <div className="flex-1 min-h-0 relative overflow-hidden">
        <SlideComp />
      </div>

      {/* Footer nav */}
      <div className="flex items-center justify-between px-8 py-3 border-t border-border/50">
        {/* Dots */}
        <div className="flex items-center gap-1.5">
          {slides.map((_, i) => (
            <button
              key={i}
              onClick={() => setCurrent(i)}
              className={`transition-all rounded-full ${i === current ? "w-6 h-1.5 bg-primary" : "w-1.5 h-1.5 bg-muted-foreground/30 hover:bg-muted-foreground/60"}`}
            />
          ))}
        </div>

        {/* Arrows */}
        <div className="flex items-center gap-2">
          <button
            onClick={() => go(-1)}
            disabled={current === 0}
            className="flex items-center gap-1 px-3 py-1.5 border border-border rounded text-xs font-mono text-muted-foreground hover:text-foreground hover:border-primary/30 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
          >
            <ChevronLeft size={14} /> prev
          </button>
          <button
            onClick={() => go(1)}
            disabled={current === total - 1}
            className="flex items-center gap-1 px-3 py-1.5 border border-border rounded text-xs font-mono text-muted-foreground hover:text-foreground hover:border-primary/30 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
          >
            next <ChevronRight size={14} />
          </button>
        </div>
      </div>
    </div>
  );
}
