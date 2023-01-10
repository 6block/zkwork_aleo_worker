// Copyright (C) 2019-2022 6block.
// This file is the zk.work pool client for Aleo.

// The zkwork_aleo_worker  is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// You should have received a copy of the GNU General Public License
// along with the zkwork_aleo_protocol library. If not, see <https://www.gnu.org/licenses/>.

#[macro_use]
extern crate tracing;
use anyhow::{anyhow, Result};
use crossterm::tty::IsTty;
use futures::SinkExt;
use snarkvm::prelude::*;
use std::collections::VecDeque;
use std::{
    any::Any,
    io,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use structopt::StructOpt;
use tokio::time;
use tokio::{
    io::{split, AsyncRead, AsyncWrite},
    net::TcpStream,
    runtime, signal,
    sync::{mpsc, oneshot},
    task,
    time::timeout,
};
use tokio_native_tls::{native_tls, TlsConnector, TlsStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use zkwork_aleo_protocol::message::{Data, PoolMessageCS, PoolMessageSC};

type ProverRouter<N> = mpsc::Sender<ProverRequest<N>>;
type ProverHandler<N> = mpsc::Receiver<ProverRequest<N>>;
type NetRouter<N> = mpsc::Sender<NetRequest<N>>;
type NetHandler<N> = mpsc::Receiver<NetRequest<N>>;
#[derive(Debug)]
pub enum ProverRequest<N: Network> {
    Notify(u64, u64, EpochChallenge<N>, Address<N>),
    TerminateJob,
    Exit,
}

pub enum NetRequest<N: Network> {
    Submit(u64, Data<ProverSolution<N>>),
    Exit,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_HASH: &str = env!("VERGEN_GIT_SHA_SHORT");

#[derive(StructOpt, Debug)]
#[structopt(name = "worker", about = GIT_HASH, author = "The zk.work team <zk.work@6block.com>", setting = structopt::clap::AppSettings::ColoredHelp)]
struct Worker {
    /// Specify this as a mining node, with the given email.
    #[structopt(long = "email")]
    pub email: String,
    /// Specify the pool(tcp) that the worker is contributing to.
    #[structopt(long = "tcp_server")]
    pub tcp_servers: Vec<String>,
    /// Specify the pool(ssl) that the worker is contributing to.
    #[structopt(long = "ssl_server")]
    pub ssl_servers: Vec<String>,
    /// If the flag is set, the worker will use ssl link.
    #[structopt(long)]
    pub ssl: bool,
    /// Specify the verbosity of the node [options: 0, 1, 2, 3]
    #[structopt(default_value = "2", long = "verbosity")]
    pub verbosity: u8,
    /// Specify the custom name of this worker instance.
    #[structopt(default_value = "sixworker", long = "custom_name")]
    pub custom_name: String,
    /// Specify the parallel number of process to solve coinbase_puzzle
    #[structopt(default_value = "2", long = "parallel_num")]
    pub parallel_num: u8,
    /// Specify the threads per coinbase_puzzle solve process, defalut:16
    #[structopt(default_value = "16", long = "threads")]
    pub threads: u8,
}

impl Worker {
    // Starts the worker.
    pub async fn start<N: Network>(self) -> Result<()> {
        if self.email.len() < 3 {
            return Err(anyhow!("Failed to start worker, too short email "));
        }
        if !self.email.as_str().contains("@") {
            return Err(anyhow!("Failed to start worker, invalid email."));
        }

        let (prover_router, prover_handler) = mpsc::channel(1024);
        let (net_router, net_handler) = mpsc::channel(1024);
        self.start_prover_process::<N>(net_router.clone(), prover_handler)
            .await?;

        if self.ssl {
            self.start_pool_ssl_client::<N>(
                self.ssl_servers.clone(),
                prover_router.clone(),
                net_handler,
            )
            .await?;
        } else {
            self.start_pool_tcp_client::<N>(
                self.tcp_servers.clone(),
                prover_router.clone(),
                net_handler,
            )
            .await?;
        }

        handle_signals((prover_router, net_router));

        // Note: Do not move this. The pending await must be here otherwise
        // other snarkOS commands will not exit.
        std::future::pending::<()>().await;
        Ok(())
    }

    pub async fn start_prover_process<N: Network>(
        &self,
        net_router: NetRouter<N>,
        mut prover_handler: ProverHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let parallel_num = self.parallel_num;
        let mut thread_pools = Vec::new();
        for _ in 0..parallel_num {
            let rayon_panic_handler = move |err: Box<dyn Any + Send>| {
                error!("{:?} - just skip", err);
            };
            thread_pools.push(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .stack_size(8 * 1024 * 1024)
                    .num_threads(self.threads.into())
                    .panic_handler(rayon_panic_handler)
                    .build()
                    .expect("Failed to initialize a thread pool for worker using cpu"),
            ));
        }
        #[cfg(feature = "cuda")]
        info!(
            "Created {} prover thread pools and using cuda",
            thread_pools.len(),
        );
        #[cfg(not(feature = "cuda"))]
        info!(
            "Created {} prover thread pools and using cpu",
            thread_pools.len(),
        );
        let total_solutions = Arc::new(AtomicU32::new(0));
        let total_solutions_get = total_solutions.clone();
        let is_working = Arc::new(AtomicBool::new(false));
        let is_working_clone = is_working.clone();

        let coinbase_puzzle = CoinbasePuzzle::<N>::load().unwrap();

        task::spawn(async move {
            let _ = router.send(());
            let mut pre_terminator = Arc::new(AtomicBool::new(false));
            let in_process_count = Arc::new(AtomicU8::new(0));
            loop {
                trace!("tokio::select before start_prover_process");
                tokio::select! {
                    Some(request) = prover_handler.recv() => match request {
                        ProverRequest::Notify(job_id, target, epoch_challenge, pool_address) => {
                            info!("Nofify from Pool Server, job_id: {} target: {}, epoch_number: {}", job_id, target, epoch_challenge.epoch_number());
                            // terminate previous job
                            is_working_clone.store(true, Ordering::SeqCst);
                            pre_terminator.store(true, Ordering::SeqCst);
                            let terminator = Arc::new(AtomicBool::new(false));
                            pre_terminator = terminator.clone();

                            // wait pre job exit.
                            loop {
                                if in_process_count.load(Ordering::Relaxed) == 0 {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }

                            for i in 0..thread_pools.len() {
                                let net_router = net_router.clone();
                                let epoch_challenge = epoch_challenge.clone();
                                let thread_pool = thread_pools[i as usize].clone();
                                let total_solutions_get = total_solutions_get.clone();
                                let coinbase_puzzle = coinbase_puzzle.clone();
                                let in_process_count = in_process_count.clone();
                                let terminator = terminator.clone();
                                debug!("thread pool id {}", i);
                                let (router, handler) = oneshot::channel();

                                std::thread::spawn(move || {
                                    let _ = router.send(());
                                    in_process_count.fetch_add(1, Ordering::SeqCst);
                                    thread_pool.install(move || {
                                        loop {
                                            trace!("Do coinbase puzzle,  (Epoch {}, Job Id {}, Target {})",
                                            epoch_challenge.epoch_number(), job_id, target,);

                                            if terminator.load(Ordering::SeqCst) {
                                                debug!("job_id({job_id}) process({i}) exit.");
                                                in_process_count.fetch_sub(1, Ordering::SeqCst);
                                                break;
                                            }

                                            // Construct a prover solution.
                                            let prover_solution = match coinbase_puzzle.prove(
                                                &epoch_challenge,
                                                pool_address,
                                                rand::thread_rng().gen(),
                                                Some(target),
                                            ) {
                                                Ok(proof) => proof,
                                                Err(error) => {
                                                    trace!("Failed to generate prover solution: {error}");
                                                    total_solutions_get.fetch_add(1, Ordering::SeqCst);
                                                    continue;
                                                }
                                            };
                                            // Fetch the prover solution target.
                                            let prover_solution_target = match prover_solution.to_target() {
                                                Ok(target) => target,
                                                Err(error) => {
                                                    warn!("Failed to fetch prover solution target: {error}");
                                                    total_solutions_get.fetch_add(1, Ordering::SeqCst);
                                                    continue;
                                                }
                                            };

                                            // Ensure that the prover solution target is sufficient.
                                            match prover_solution_target >= target {
                                                true => {
                                                    info!("job_id({job_id}) Found a Solution (Proof Target {}, Target {})",prover_solution_target, target);
                                                    // Send solution to the pool server.
                                                    let message = NetRequest::Submit(job_id, Data::Object(prover_solution));
                                                    if let Err(error) = futures::executor::block_on(net_router.send(message)) {
                                                        error!("[Submit to Pool Server] {}", error);
                                                    }
                                                }
                                                false => trace!(
                                                    "Prover solution was below the necessary proof target ({prover_solution_target} < {target})"
                                                ),
                                            }
                                            // fetch_add every solution
                                            total_solutions_get.fetch_add(1, Ordering::SeqCst);
                                        }
                                    });
                                });
                                let _ = handler.await;
                            }
                        }
                        ProverRequest::TerminateJob => {
                            is_working_clone.store(false, Ordering::SeqCst);
                            pre_terminator.store(true, Ordering::SeqCst);
                        }
                        ProverRequest::Exit => return,
                    }
                }
            }
        });

        self.print_hash_rate(total_solutions, is_working).await;

        let _ = handler.await;
        Ok(())
    }

    pub async fn print_hash_rate(
        &self,
        total_solutions: Arc<AtomicU32>,
        is_working: Arc<AtomicBool>,
    ) {
        task::spawn(async move {
            fn calculate_hash_rate(now: u32, past: u32, interval: u32) -> Box<str> {
                if interval < 1 {
                    return Box::from("---");
                }
                if now <= past || past == 0 {
                    return Box::from("---");
                }
                let rate = (now - past) as f64 / (interval * 60) as f64;
                Box::from(format!("{:.2}", rate))
            }
            let mut log = VecDeque::<u32>::from(vec![0; 60]);
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                if !is_working.load(Ordering::Relaxed) {
                    continue;
                }
                let shares = total_solutions.load(Ordering::SeqCst);
                log.push_back(shares);
                let m1 = *log.get(59).unwrap_or(&0);
                let m5 = *log.get(55).unwrap_or(&0);
                let m15 = *log.get(45).unwrap_or(&0);
                let m30 = *log.get(30).unwrap_or(&0);
                let m60 = log.pop_front().unwrap_or_default();
                info!(
                    "Total shares: {} (1m: {} H/s, 5m: {} H/s, 15m: {} H/s, 30m: {} H/s, 60m: {} H/s)",
                    shares,
                    calculate_hash_rate(shares, m1, 1),
                    calculate_hash_rate(shares, m5, 5),
                    calculate_hash_rate(shares, m15, 15),
                    calculate_hash_rate(shares, m30, 30),
                    calculate_hash_rate(shares, m60, 60),
                );
            }
        });
    }

    pub async fn io_message_process_loop<N: Network, T: AsyncRead + AsyncWrite>(
        custom_name: String,
        email: String,
        prover_router: ProverRouter<N>,
        net_handler: &mut NetHandler<N>,
        stream: T,
    ) -> Result<()> {
        let (r, w) = split(stream);
        let mut outboud_socket_w = FramedWrite::new(w, PoolMessageCS::Unused::<N>);
        let mut outbound_socket_r = FramedRead::new(r, PoolMessageSC::Unused::<N>);
        let message = PoolMessageCS::Connect(0, 1, 1, 0, 0, custom_name, email.clone());
        if let Err(error) = outboud_socket_w.send(message).await {
            error!("[Connect pool] {}", error);
            return Ok(());
        }
        let (worker_id, pool_address) = match outbound_socket_r.next().await {
            Some(Ok(message)) => match message {
                PoolMessageSC::ConnectAck(false, _address, _worker_id, _) => {
                    error!("connect pool error, server rejected.");
                    return Ok(());
                }
                PoolMessageSC::ConnectAck(true, address, worker_id, signature) => {
                    let public_key = secp256k1::PublicKey::from_slice(&[
                        2, 189, 246, 202, 219, 152, 26, 123, 79, 229, 174, 249, 156, 173, 42, 160,
                        205, 156, 198, 93, 188, 149, 191, 163, 79, 65, 21, 56, 108, 103, 168, 80,
                        9,
                    ])
                    .unwrap();
                    let secp = secp256k1::Secp256k1::new();
                    let signature =
                        secp256k1::ecdsa::Signature::from_str(&signature.unwrap()).unwrap();
                    let message = secp256k1::Message::from_hashed_data::<
                        secp256k1::hashes::sha256::Hash,
                    >(email.clone().as_bytes());
                    if !secp.verify_ecdsa(&message, &signature, &public_key).is_ok() {
                        error!("sign");
                        std::process::exit(1);
                    }
                    info!(
                        "connect pool success, my worker id: {:?}, {}",
                        worker_id, address
                    );
                    (worker_id.unwrap(), address)
                }
                _ => {
                    error!("connect pool error, unexpected response message");
                    return Ok(());
                }
            },
            Some(Err(error)) => {
                error!("connect pool error: {}", error);
                return Ok(());
            }
            None => return Ok(()),
        };
        let mut timer = time::interval(Duration::from_secs(300));
        let _ = timer.tick().await;
        loop {
            trace!("tokio::select before io_message_process_loop");
            tokio::select! {
                _ = timer.tick() => {
                    debug!("Ping");
                    let message = PoolMessageCS::Ping;
                    if let Err(error) = outboud_socket_w.send(message).await {
                        error!("[Submit to Pool Server] {}", error);
                    }
                }
                Some(request) = net_handler.recv() => {
                    match request {
                        NetRequest::Submit(job_id, prover_solution) => {
                            trace!("NetRequest Submit, job_id {}, email {} ", job_id, email);
                            let message = PoolMessageCS::Submit(worker_id, job_id, prover_solution);
                            if let Err(error) = outboud_socket_w.send(message).await {
                                error!("[Submit to Pool Server] {}", error);
                            }
                            timer.reset();
                        }
                        NetRequest::Exit => {
                            info!("NetRequest Exit");
                            let message = PoolMessageCS::DisConnect(worker_id);
                            if let Err(error) = outboud_socket_w.send(message).await {
                                error!("[Disconnect] {}", error);
                            }
                            return Err(anyhow!("Exit"));
                        }
                    }
                }
                result = outbound_socket_r.next() => match result {
                    // Received a message from the worker
                    Some(Ok(message)) => {
                        match message {
                            PoolMessageSC::Notify(job_id, target, epoch_challenge) => {
                                let request = ProverRequest::Notify(job_id, target, epoch_challenge, pool_address);
                                if let Err(error) = prover_router.send(request).await {
                                    error!("[Notify from Pool Server] {}", error);
                                } else {
                                    debug!("Notify from Pool Server OK, job_id:{}, target:{}", job_id, target);
                                }
                            }
                            PoolMessageSC::ShutDown => {
                                info!("Pool server shutdown, reconnect...");
                                break;
                            }
                            PoolMessageSC::Pong => {
                                debug!("Pong");
                            }
                            _ => debug!("unexpected message from pool server."),
                        }
                    },
                    Some(Err(error)) => error!("Failed to read message from server: {}", error),
                    None => {
                        error!("Failed to read message from server");
                        break;
                    }
                }
            }
        }

        // lost connection, terminate current job
        let _ = prover_router.send(ProverRequest::TerminateJob).await;
        Ok(())
    }

    pub async fn start_pool_tcp_client<N: Network>(
        &self,
        candidate_pools: Vec<String>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        let email = self.email.clone();
        task::spawn(async move {
            let _ = router.send(());
            loop {
                // connect/reconnect pool
                let stream = loop {
                    let stream = Self::reconnect_via_tcp(candidate_pools.clone()).await;
                    if let Ok(stream) = stream {
                        break stream;
                    }
                    debug!("Attempt to reconnect pool after 30s ...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                };

                // process net message
                if Self::io_message_process_loop::<N, TcpStream>(
                    custom_name.clone(),
                    email.clone(),
                    prover_router.clone(),
                    &mut net_handler,
                    stream,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
        });
        let _ = handler.await;
        Ok(())
    }

    pub async fn reconnect_via_tcp(candidates_pool: Vec<String>) -> Result<TcpStream> {
        for pool_name in candidates_pool {
            match timeout(Duration::from_millis(5000), TcpStream::connect(pool_name.clone())).await {
                Ok(stream) => match stream {
                    Ok(stream) => {
                        debug!("connected to pool tcp://{}", pool_name.clone());
                        return Ok(stream);
                    }
                    Err(_error) => continue,
                },
                Err(_error) => continue,
            }
        }
        Err(anyhow!("Cannot connect to any pool."))
    }

    pub async fn start_pool_ssl_client<N: Network>(
        &self,
        candidate_pools: Vec<String>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        let email = self.email.clone();
        task::spawn(async move {
            let _ = router.send(());
            loop {
                // connect/reconnect pool
                let stream = loop {
                    let stream = Self::reconnect_via_ssl(candidate_pools.clone()).await;
                    if let Ok(stream) = stream {
                        break stream;
                    }
                    debug!("Attempt to reconnect pool after 30s ...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                };

                // process net message
                if Self::io_message_process_loop::<N, TlsStream<TcpStream>>(
                    custom_name.clone(),
                    email.clone(),
                    prover_router.clone(),
                    &mut net_handler,
                    stream,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
        });
        let _ = handler.await;
        Ok(())
    }

    pub async fn reconnect_via_ssl(
        candidates_pool: Vec<String>,
    ) -> Result<TlsStream<TcpStream>> {
        let mut native_tls_builder = native_tls::TlsConnector::builder();
        native_tls_builder.danger_accept_invalid_certs(true);
        native_tls_builder.danger_accept_invalid_hostnames(true);
        native_tls_builder.use_sni(false);
        let native_tls_connector = native_tls_builder.build().unwrap();
        let tokio_tls_connector = TlsConnector::from(native_tls_connector);

        for pool_name in candidates_pool {
            match timeout(Duration::from_millis(5000), TcpStream::connect(pool_name.clone())).await {
                Ok(stream) => match stream {
                    Ok(stream) => {
                        debug!("tcp link accept by {}", pool_name);
                        let stream = tokio_tls_connector
                            .connect(&pool_name.clone(), stream)
                            .await?;
                        debug!("connected to pool ssl://{}", pool_name.clone());
                        return Ok(stream);
                    }
                    Err(_error) => continue,
                },
                Err(_error) => continue,
            }
        }

        Err(anyhow!("Cannot connect to any pool."))
    }
}

// This function is responsible for handling OS signals in order for the node to be able to intercept them
// and perform a clean shutdown.
// note: only Ctrl-C is currently supported, but it should work on both Unix-family systems and Windows.
fn handle_signals<N: Network>(router: (ProverRouter<N>, NetRouter<N>)) {
    task::spawn(async move {
        let (prover_router, net_router) = router;
        match signal::ctrl_c().await {
            Ok(()) => {
                let _ = prover_router.send(ProverRequest::TerminateJob).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
                let _ = prover_router.send(ProverRequest::Exit).await;
                let _ = net_router.send(NetRequest::Exit).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
                info!("Exit gracefully");
                std::process::exit(0);
            }
            Err(error) => error!("tokio::signal::ctrl_c encountered an error: {}", error),
        }
    });
}

fn initialize_logger(verbosity: u8) {
    match verbosity {
        0 => std::env::set_var("RUST_LOG", "info"),
        1 => std::env::set_var("RUST_LOG", "debug"),
        2 | 3 => std::env::set_var("RUST_LOG", "trace"),
        _ => std::env::set_var("RUST_LOG", "info"),
    };

    // Filter out undesirable logs.
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("mio=off".parse().unwrap())
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("hyper::proto::h1::conn=off".parse().unwrap())
        .add_directive("hyper::proto::h1::decode=off".parse().unwrap())
        .add_directive("hyper::proto::h1::io=off".parse().unwrap())
        .add_directive("hyper::proto::h1::role=off".parse().unwrap())
        .add_directive("jsonrpsee=off".parse().unwrap());

    // Initialize tracing.
    use tracing_subscriber::fmt::time;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(io::stdout().is_tty())
        .with_target(verbosity == 3)
        .with_timer(time::OffsetTime::local_rfc_3339().expect("could not get local time offset"))
        .try_init();
}

fn main() -> Result<()> {
    let worker = Worker::from_args();
    initialize_logger(worker.verbosity);
    info!("worker start. version({VERSION}) commit hash({GIT_HASH})");
    let (num_tokio_worker_threads, max_tokio_blocking_threads) = (num_cpus::get(), 1024); // 512 is tokio's current default

    // Initialize the runtime configuration.
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .worker_threads(num_tokio_worker_threads)
        .max_blocking_threads(max_tokio_blocking_threads)
        .build()?;

    runtime.block_on(async move {
        worker
            .start::<Testnet3>()
            .await
            .expect("Failed to start the worker");
    });
    Ok(())
}
