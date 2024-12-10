use core::fmt::Debug;
use core::num::NonZeroUsize;

use core::str::FromStr;
use std::env::{self, VarError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{process, thread};

use anyhow::{anyhow, Context as _};
use clap::Parser;
use nix::sched::{unshare, CloneFlags};
use tokio::sync::{broadcast, oneshot};
use tokio::{fs, join, try_join};
use wasmtime::component::{Component, Linker};
use wasmtime::{InstanceAllocationStrategy, PoolingAllocationConfig, Store};
use wasmtime_wasi::bindings::CommandPre;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

/// Run containerized Wasm on a Linux system.
#[derive(Parser, Debug)]
pub struct Args {
    /// Amount of cgroups/namespaces to create.
    ///
    /// If not set, a reasonable value will be computed using active resource limits
    #[clap(long, short)]
    count: Option<NonZeroUsize>,

    /// Cgroup path to use, value derived from `/proc/self/cgroup` will be used otherwise
    #[clap(long)]
    cgroup: Option<PathBuf>,

    /// Path to a Wasm command component to use
    wasm: PathBuf,
}

fn getenv<T>(key: &str) -> Option<T>
where
    T: FromStr,
    T::Err: Debug,
{
    match env::var(key).as_deref().map(FromStr::from_str) {
        Ok(Ok(v)) => Some(v),
        Ok(Err(err)) => {
            eprintln!("failed to parse `{key}` value, ignoring: {err:?}");
            None
        }
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(..)) => {
            eprintln!("`{key}` value is not valid UTF-8, ignoring");
            None
        }
    }
}

fn new_pooling_config(instances: u32) -> PoolingAllocationConfig {
    let mut config = PoolingAllocationConfig::default();
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_UNUSED_WASM_SLOTS") {
        config.max_unused_warm_slots(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_DECOMMIT_BATCH_SIZE") {
        config.decommit_batch_size(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_ASYNC_STACK_ZEROING") {
        config.async_stack_zeroing(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_ASYNC_STACK_KEEP_RESIDENT") {
        config.async_stack_keep_resident(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_LINEAR_MEMORY_KEEP_RESIDENT") {
        config.linear_memory_keep_resident(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TABLE_KEEP_RESIDENT") {
        config.table_keep_resident(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_COMPONENT_INSTANCES") {
        config.total_component_instances(v);
    } else {
        config.total_component_instances(instances);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_COMPONENT_INSTANCE_SIZE") {
        config.max_component_instance_size(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_CORE_INSTANCES_PER_COMPONENT") {
        config.max_core_instances_per_component(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_MEMORIES_PER_COMPONENT") {
        config.max_memories_per_component(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_TABLES_PER_COMPONENT") {
        config.max_tables_per_component(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_MEMORIES") {
        config.total_memories(v);
    } else {
        config.total_memories(instances);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_TABLES") {
        config.total_tables(v);
    } else {
        config.total_tables(instances);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_STACKS") {
        config.total_stacks(v);
    } else {
        config.total_stacks(instances);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_CORE_INSTANCES") {
        config.total_core_instances(v);
    } else {
        config.total_core_instances(instances);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_CORE_INSTANCE_SIZE") {
        config.max_core_instance_size(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_TABLES_PER_MODULE") {
        config.max_tables_per_module(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_TABLE_ELEMENTS") {
        config.table_elements(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_MEMORIES_PER_MODULE") {
        config.max_memories_per_module(v);
    }
    if let Some(v) = getenv("WASMTIME_POOLING_MAX_MEMORY_SIZE") {
        config.max_memory_size(v);
    }
    // TODO: Add memory protection key support
    if let Some(v) = getenv("WASMTIME_POOLING_TOTAL_GC_HEAPS") {
        config.total_gc_heaps(v);
    } else {
        config.total_gc_heaps(instances);
    }
    config
}

// https://github.com/bytecodealliance/wasmtime/blob/b943666650696f1eb7ff8b217762b58d5ef5779d/src/commands/serve.rs#L641-L656
fn use_pooling_allocator_by_default() -> anyhow::Result<bool> {
    const BITS_TO_TEST: u32 = 42;
    if let Some(v) = getenv("WASMTIME_POOLING") {
        return Ok(v);
    }
    let mut config = wasmtime::Config::new();
    config.wasm_memory64(true);
    config.static_memory_maximum_size(1 << BITS_TO_TEST);
    let engine = wasmtime::Engine::new(&config)?;
    let mut store = wasmtime::Store::new(&engine, ());
    // NB: the maximum size is in wasm pages to take out the 16-bits of wasm
    // page size here from the maximum size.
    let ty = wasmtime::MemoryType::new64(0, Some(1 << (BITS_TO_TEST - 16)));
    Ok(wasmtime::Memory::new(&mut store, ty).is_ok())
}

pub struct Ctx {
    pub table: ResourceTable,
    pub wasi: WasiCtx,
    pub http: WasiHttpCtx,
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiHttpView for Ctx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

fn main() -> anyhow::Result<()> {
    let Args {
        count,
        wasm,
        cgroup,
    } = Args::parse();

    unshare(CloneFlags::CLONE_NEWUSER).context("failed to unshare user namespace")?;

    let pid = process::id();
    let nofile = rlimit::Resource::NOFILE
        .get_soft()
        .context("failed to get `NOFILE` rlimit")?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_name("cgwasm")
        .build()
        .context("failed to build root Tokio runtime")?;
    let rt = rt.handle();
    rt.block_on(async move {
        let (cg, wasm) = try_join!(
            async {
                if let Some(cgroup) = cgroup {
                    Ok(cgroup)
                } else {
                    let cg = fs::read_to_string("/proc/self/cgroup")
                        .await
                        .context("failed to read `/proc/self/cgroup`")?;
                    let cg = cg
                        .trim()
                        .strip_prefix("0::/")
                        .context("process does not run within cgroup v2")?;
                    Ok(Path::new("/sys/fs/cgroup").join(cg))
                }
            },
            async {
                fs::read(&wasm)
                    .await
                    .with_context(|| format!("failed to read `{}`", wasm.display()))
            }
        )?;

        let count = if let Some(count) = count {
            count.into()
        } else {
            let pids_current_path = cg.join("pids.current");
            let pids_max_path = cg.join("pids.max");
            let (threads_max, pids_current, pids_max) = join!(
                async {
                    let threads_max = fs::read_to_string("/proc/sys/kernel/threads-max")
                        .await
                        .context("failed to read `/proc/sys/kernel/threads-max`")?;
                    threads_max
                        .trim()
                        .parse::<usize>()
                        .context("failed to parse `/proc/sys/kernel/threads-max` contents")
                },
                fs::read_to_string(&pids_current_path),
                fs::read_to_string(&pids_max_path),
            );
            let threads_max = threads_max?;
            let nproc = rlimit::Resource::NPROC
                .get_soft()
                .context("failed to get `NPROC` rlimit")?;
            eprintln!("threads-max: {threads_max}");
            eprintln!("NPROC: {nproc}");
            let mut count = threads_max
                .min(nproc.try_into().unwrap_or(usize::MAX))
                .min(nofile.try_into().unwrap_or(usize::MAX));
            match pids_max.as_deref().map(str::trim) {
                Ok("max") => {
                    eprintln!("pids.max: max");
                }
                Ok(pids_max) => {
                    eprintln!("pids.max: {pids_max}");
                    let pids_max = pids_max.parse::<usize>().with_context(|| {
                        format!("failed to parse `{}` contents", pids_max_path.display())
                    })?;
                    match pids_current {
                        Ok(pids_current) => {
                            eprintln!("pids.current: {pids_current}");
                            let pids_current =
                                pids_current.trim().parse::<usize>().with_context(|| {
                                    format!(
                                        "failed to parse `{}` contents",
                                        pids_current_path.display()
                                    )
                                })?;
                            count = count.min(pids_max.saturating_sub(pids_current));
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            count = count.min(pids_max);
                        }
                        Err(err) => {
                            eprintln!(
                                "failed to read `{}` contents: {err}",
                                pids_current_path.display()
                            )
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    eprintln!(
                        "failed to read `{}` contents: {err}",
                        pids_max_path.display()
                    )
                }
            }
            count.saturating_div(4).max(1)
        };
        eprintln!(
            "PID: {pid}, NOFILE: {nofile}, count: {count}, cgroup: {}",
            cg.display()
        );

        let controllers = fs::read_to_string(cg.join("cgroup.controllers"))
            .await
            .context("failed to read `cgroup.controllers`")?;
        eprintln!("cgroup.controllers: {controllers}");
        let controllers = controllers.split_whitespace().fold(
            String::with_capacity("+cpuset +cpu +pids".len()),
            |mut s, c| {
                if c == "cpuset" {
                    if s.is_empty() {
                        s.push_str("+cpuset")
                    } else {
                        s.push_str(" +cpuset")
                    }
                } else if c == "cpu" {
                    if s.is_empty() {
                        s.push_str("+cpu")
                    } else {
                        s.push_str(" +cpu")
                    }
                } else if c == "pids" {
                    if s.is_empty() {
                        s.push_str("+pids")
                    } else {
                        s.push_str(" +pids")
                    }
                }
                s
            },
        );
        fs::write(cg.join("cgroup.subtree_control"), &controllers)
            .await
            .context("failed to enable threaded controllers in `cgwasm` cgroup")?;

        let cg = cg.join("cgwasm");
        fs::create_dir_all(&cg)
            .await
            .context("failed to create `cgwasm` cgroup")?;
        fs::write(cg.join("cgroup.type"), "threaded")
            .await
            .context("failed to set `cgwasm` group type to `threaded`")?;
        fs::write(cg.join("cgroup.procs"), pid.to_string())
            .await
            .context("failed to add PID to `cgwasm` cgroup")?;
        fs::write(cg.join("cgroup.subtree_control"), &controllers)
            .await
            .context("failed to enable threaded controllers in `cgwasm` cgroup")?;

        let mut engine_config = wasmtime::Config::default();
        engine_config.wasm_component_model(true);
        engine_config.async_support(true);
        if let Ok(true) = use_pooling_allocator_by_default() {
            engine_config.allocation_strategy(InstanceAllocationStrategy::Pooling(
                new_pooling_config(count.saturating_mul(4).try_into().unwrap_or(u32::MAX)),
            ));
        } else {
            engine_config.allocation_strategy(InstanceAllocationStrategy::OnDemand);
        }
        if let Some(v) = getenv("WASMTIME_DEBUG_INFO") {
            engine_config.debug_info(v);
        }
        if let Some(v) = getenv("WASMTIME_MAX_WASM_STACK") {
            engine_config.max_wasm_stack(v);
        }
        if let Some(v) = getenv("WASMTIME_ASYNC_STACK_SIZE") {
            engine_config.async_stack_size(v);
        }
        let engine =
            match wasmtime::Engine::new(&engine_config).context("failed to construct engine") {
                Ok(engine) => engine,
                Err(err) => {
                    eprintln!("failed to construct engine, fallback to on-demand allocator: {err}");
                    engine_config.allocation_strategy(InstanceAllocationStrategy::OnDemand);
                    wasmtime::Engine::new(&engine_config).context("failed to construct engine")?
                }
            };

        let cg: Arc<Path> = cg.into_boxed_path().into();
        let (wasm_tx, _) = broadcast::channel(1);
        let mut tasks = Vec::with_capacity(count);
        for i in 0..count {
            let name = format!("cgwasm_sandbox_{i}");
            let engine = engine.clone();
            let cg = cg.join(&name);
            let mut wasm_rx = wasm_tx.subscribe();
            let (done_tx, done_rx) = oneshot::channel();
            let Ok(task) = thread::Builder::new().name(name.clone()).spawn({
                let name = name.clone();
                move || {
                    let tid = unsafe { libc::gettid() };
                    std::fs::create_dir_all(&cg)
                        .with_context(|| format!("failed to create `{name}` cgroup"))?;
                    let path = cg.join("cgroup.type");
                    std::fs::write(&path, b"threaded").with_context(|| {
                        format!("failed to write `threaded` to `{}`", path.display())
                    })?;
                    let path = cg.join("cgroup.threads");
                    std::fs::write(&path, tid.to_string()).with_context(|| {
                        format!("failed to write `{tid}` to `{}`", path.display())
                    })?;
                    unshare(
                        CloneFlags::CLONE_NEWIPC
                            | CloneFlags::CLONE_NEWNET
                            | CloneFlags::CLONE_NEWNS
                            | CloneFlags::CLONE_NEWUTS,
                    )
                    .context("failed to unshare thread")?;
                    // TODO: `pivot_root` etc.
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .enable_time()
                        .thread_name(name.clone())
                        .build()
                        .with_context(|| format!("failed to build runtime for sandbox {name}"))?;

                    let res = rt.block_on(async {
                        let wasm: CommandPre<Ctx> =
                            wasm_rx.recv().await.context("Wasm sender closed")?;
                        let mut store = Store::new(
                            &engine,
                            Ctx {
                                wasi: WasiCtxBuilder::new()
                                    .inherit_env()
                                    .inherit_stdio()
                                    .inherit_network()
                                    .allow_ip_name_lookup(true)
                                    .allow_tcp(true)
                                    .allow_udp(true)
                                    .args(&["main.wasm".to_string()])
                                    .build(),
                                http: WasiHttpCtx::new(),
                                table: ResourceTable::new(),
                            },
                        );
                        let wasm = wasm
                            .instantiate_async(&mut store)
                            .await
                            .context("failed to instantiate the component")?;
                        let res = wasm
                            .wasi_cli_run()
                            .call_run(&mut store)
                            .await
                            .context("failed to run component")?;
                        anyhow::Ok(res)
                    });
                    done_tx
                        .send(())
                        .map_err(|_| anyhow!("done receiver closed"))?;
                    anyhow::Ok(res)
                }
            }) else {
                eprintln!("failed to create thread {i}, stop");
                break;
            };
            tasks.push(rt.spawn(async move {
                _ = done_rx.await;
                eprintln!("joining thread...");
                let res = task
                    .join()
                    .map_err(|_| anyhow!("thread panicked"))?
                    .context("thread failed")?;
                eprintln!("task completed: {res:?}");
                anyhow::Ok(())
            }));
        }
        let component = Component::new(&engine, wasm).context("failed to compile component")?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_async(&mut linker).context("failed to link WASI")?;
        wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
            .context("failed to link `wasi:http`")?;
        let pre = linker
            .instantiate_pre(&component)
            .context("failed to pre-instantiate component")?;
        let pre = CommandPre::new(pre).context("component does not export `wasi:cli/command`")?;
        wasm_tx
            .send(pre)
            .map_err(|_| anyhow!("Wasm receiver closed"))?;
        for task in tasks {
            eprintln!("joining task...");
            task.await.context("task panicked")??
        }
        anyhow::Ok(())
    })
}
