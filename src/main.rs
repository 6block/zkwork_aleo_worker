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
use futures::SinkExt;
use snarkvm::{prelude::*};
use snarkos::{
        initialize_logger,
        message::Data,
    };
use std::{
    any::Any,
    convert::TryFrom,
    fs::File,
    io::{self, BufReader},
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use std::collections::VecDeque;
use structopt::StructOpt;
use tokio::{
    io::{split, AsyncRead, AsyncWrite},
    net::TcpStream,
    runtime,
    signal,
    sync::{mpsc, oneshot},
    task,
    time::timeout,
};
use tokio_rustls::{
    client::TlsStream,
    rustls::{self, OwnedTrustAnchor},
    webpki,
    TlsConnector,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use zkwork_aleo_protocol::{
        message::{PoolMessageCS, PoolMessageSC},
    };

type ProverRouter<N> = mpsc::Sender<ProverRequest<N>>;
type ProverHandler<N> = mpsc::Receiver<ProverRequest<N>>;
type NetRouter<N> = mpsc::Sender<NetRequest<N>>;
type NetHandler<N> = mpsc::Receiver<NetRequest<N>>;
#[derive(Debug)]
pub enum ProverRequest<N: Network> {
    Notify(u64, u64, EpochChallenge<N>),
    TerminateJob,
    Exit,
}

pub enum NetRequest<N: Network> {
    Submit(u64, Address<N>, Data<ProverSolution<N>>),
    Exit,
}
#[derive(StructOpt, Debug)]
#[structopt(name = "worker", author = "The Aleo Team <hello@aleo.org>", setting = structopt::clap::AppSettings::ColoredHelp)]
struct Worker {
    /// Specify this as a mining node, with the given miner address.
    #[structopt(long = "address")]
    pub address: Option<String>,
    /// Specify the pool(tcp) that the worker is contributing to.
    #[structopt(long = "tcp_server")]
    pub tcp_servers: Vec<SocketAddr>,
    /// Specify the pool(ssl) that the worker is contributing to.
    #[structopt(long = "ssl_server")]
    pub ssl_servers: Vec<SocketAddr>,
    /// If the flag is set, the worker will use ssl link.
    #[structopt(long)]
    pub ssl: bool,
    /// Specify the verbosity of the node [options: 0, 1, 2, 3]
    #[structopt(default_value = "2", long = "verbosity")]
    pub verbosity: u8,
    /// Specify the custom name of this worker instance.
    #[structopt(default_value = "sixworker", long = "custom_name")]
    pub custom_name: String,
    /// Specify the parallel number of posw
    #[structopt(default_value = "2", long = "parallel_num")]
    pub parallel_num: u8,

    #[structopt(verbatim_doc_comment)]
    /// Indexes of GPUs to use (starts from 0)
    /// Specify multiple times to use multiple GPUs
    /// Example: -g 0 -g 1 -g 2
    /// Note: Pure CPU proving will be disabled as each GPU job requires one CPU thread as well
    #[structopt(short = "g", long = "cuda")]
    pub cuda: Option<Vec<i16>>,

    #[structopt(verbatim_doc_comment)]
    /// Parallel jobs per GPU, defaults to 1
    /// Example: -g 0 -g 1 -j 4
    /// The above example will result in 8 jobs in total
    #[structopt(short = "j", long = "cuda-jobs")]
    pub jobs: Option<u8>,
}

impl Worker {
    // Starts the worker.
    pub async fn start<N: Network>(self) -> Result<()> {
        let address = match self.address {
            Some(ref address) => {
                let address = Address::<N>::from_str(address)?;
                info!("Worker address is {}.\n", address);
                Some(address)
            }
            None => None,
        };
        match address {
            Some(address) => {
                let (prover_router, prover_handler) = mpsc::channel(1024);
                let (net_router, net_handler) = mpsc::channel(1024);
                self.start_prover_process::<N>(address, net_router.clone(), prover_handler)
                    .await?;

                if self.ssl {
                    self.start_pool_ssl_client::<N>(self.ssl_servers.clone(), prover_router.clone(), net_handler)
                        .await?;
                } else {
                    self.start_pool_tcp_client::<N>(self.tcp_servers.clone(), prover_router.clone(), net_handler)
                        .await?;
                }

                handle_signals((prover_router, net_router));
            }
            None => return Err(anyhow!("Failed to start worker, lose address")),
        };

        // Note: Do not move this. The pending await must be here otherwise
        // other snarkOS commands will not exit.
        std::future::pending::<()>().await;
        Ok(())
    }

    pub async fn start_prover_process<N: Network>(
        &self,
        address: Address<N>,
        net_router: NetRouter<N>,
        mut prover_handler: ProverHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let parallel_num = self.parallel_num;
        let mut thread_pools = Vec::new();
        if self.cuda.clone().is_none() {
            info!("cuda.is_none {:?}", self.cuda.clone().is_none());
            for _ in 0..parallel_num {
                let rayon_panic_handler = move |err: Box<dyn Any + Send>| {
                    error!("{:?} - just skip", err);
                };
                thread_pools.push(Arc::new(rayon::ThreadPoolBuilder::new()
                                        .stack_size(8 * 1024 *1024)
                                        .num_threads((num_cpus::get() * 3 / 2 / parallel_num as usize).max(2))
                                        .panic_handler(rayon_panic_handler)
                                        .build()
                                        .expect("Failed to initialize a thread pool for worker using cpu")
                                        ));
            }
        } else {
            info!("cuda {:?} cuda_jobs {:?}", self.cuda.clone(), self.jobs.clone().unwrap_or(1));
            let total_jobs = self.jobs.clone().unwrap_or(1) * self.cuda.clone().unwrap().len() as u8;
            for index in 0..total_jobs {
                let rayon_panic_handler = move |err: Box<dyn Any + Send>| {
                    error!("{:?} - just skip", err);
                };
                thread_pools.push(Arc::new(rayon::ThreadPoolBuilder::new()
                                        .stack_size(8 * 1024 *1024)
                                        .num_threads(2)
                                        .panic_handler(rayon_panic_handler)
                                        .thread_name(move |idx| format!("ap-cuda-{}-{}", index, idx))
                                        .build()
                                        .expect("Failed to initialize a thread pool for worker using cuda")
                                        ));
            }
        }
        info!(
            "Created {} prover thread pools and using cuda is {}",
            thread_pools.len(),
            self.cuda.clone().is_some(),
        );
        let total_shares = Arc::new(AtomicU32::new(0));
        let total_shares_get = total_shares.clone();

        task::spawn(async move {
            let _ = router.send(());
            let mut terminator_previous = Arc::new(AtomicBool::new(false));
            let running_posw_count = Arc::new(AtomicU8::new(0)); // todo rename
            loop {
                info!("tokio::select before start_prover_process");
                tokio::select! {
                    Some(request) = prover_handler.recv() => match request {
                        ProverRequest::Notify(job_id, target, epoch_challenge) => {
                            //Terminate previous job
                            terminator_previous.store(true, Ordering::SeqCst);
                            let terminator = Arc::new(AtomicBool::new(false));
                            terminator_previous = terminator.clone();
                            info!("Nofify from Pool Server, job_id: {} target: {}, epoch_challenge: {}", job_id, target, epoch_challenge.epoch_number());
                            loop {
                                if running_posw_count.load(Ordering::Relaxed) == 0 {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            for i in 0..thread_pools.len() {
                                let net_router = net_router.clone();
                                let terminator = terminator.clone();

                                let epoch_challenge = epoch_challenge.clone();
                                let thread_pool = thread_pools[i as usize].clone();
                                let running_posw_count = running_posw_count.clone();
                                let total_shares_get = total_shares_get.clone();
                                trace!("thread pool id {}", i);
                                let (router, handler) = oneshot::channel();

                                task::spawn(async move {
                                    let _ = router.send(());
                                    running_posw_count.fetch_add(1, Ordering::SeqCst);
                                    tokio::time::sleep(Duration::from_millis(i as u64 * 200)).await;
                                    loop {
                                        let terminator_clone = terminator.clone();

                                        let epoch_challenge = epoch_challenge.clone();
                                        let thread_pool = thread_pool.clone();
                                        if terminator.load(Ordering::SeqCst) {
                                            trace!("posw {} terminator", i);
                                            running_posw_count.fetch_sub(1, Ordering::SeqCst);
                                            break;
                                        }
                                        // let result = task::spawn_blocking(move || {
                                        //     thread_pool.install(move || {
                                                loop {
                                                    info!("Do coinbase puzzle,  (Epoch {}, Job Id {}, Target {})",
                                                    epoch_challenge.epoch_number(), job_id, target,);

                                                    let coinbase_puzzle = CoinbasePuzzle::<N>::load();

                                                    // Construct a prover solution.
                                                    let prover_solution = match coinbase_puzzle.unwrap().prove(//tbd
                                                        &epoch_challenge,
                                                        address,
                                                        rand::thread_rng().gen(),
                                                        Some(target),
                                                    ) {
                                                        Ok(proof) => proof,
                                                        Err(error) => {
                                                            warn!("Failed to generate prover solution: {error}");
                                                            break;
                                                        }
                                                    };

                                                    // Fetch the prover solution target.
                                                    let prover_solution_target = match prover_solution.to_target() {
                                                        Ok(target) => target,
                                                        Err(error) => {
                                                            warn!("Failed to fetch prover solution target: {error}");
                                                            break;
                                                        }
                                                    };

                                                    // Ensure that the prover solution target is sufficient.
                                                    match prover_solution_target >= target {
                                                        true => {
                                                            info!("Found a Solution (Proof Target {}, Target {})",prover_solution_target, target);

                                                            // Send solution to the pool server.
                                                            let message = NetRequest::Submit(job_id, address, Data::Object(prover_solution));
                                                            if let Err(error) = net_router.send(message).await {
                                                                error!("[Submit to Pool Server] {}", error);
                                                            }
                                                            total_shares_get.fetch_add(1, Ordering::SeqCst);

                                                            break;
                                                        }
                                                        false => trace!(
                                                            "Prover solution was below the necessary proof target ({prover_solution_target} < {target})"
                                                        ),
                                                    }
                                                }
                                        if terminator.load(Ordering::SeqCst) {
                                            trace!("posw {} terminator", i);
                                            running_posw_count.fetch_sub(1, Ordering::SeqCst);
                                            break;
                                        }
                                    }
                                });
                                let _ = handler.await;
                            }
                        }
                        ProverRequest::TerminateJob => terminator_previous.store(true, Ordering::SeqCst),
                        ProverRequest::Exit => return,
                    }
                }
            }
        });

        self.print_hash_rate(total_shares.clone()).await;

        let _ = handler.await;
        Ok(())
    }

    pub async fn print_hash_rate(&self, total_shares_cal: Arc<AtomicU32>) {
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
                let shares = total_shares_cal.load(Ordering::SeqCst);
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
        prover_router: ProverRouter<N>,
        net_handler: &mut NetHandler<N>,
        stream: T,
    ) -> Result<()> {
        let (r, w) = split(stream);
        let mut outboud_socket_w = FramedWrite::new(w, PoolMessageCS::Unused::<N>);
        let mut outbound_socket_r = FramedRead::new(r, PoolMessageSC::Unused::<N>);
        let message = PoolMessageCS::Connect(0, 1, 0, 0, custom_name);
        if let Err(error) = outboud_socket_w.send(message).await {
            error!("[Connect pool] {}", error);
            return Ok(());
        }
        let worker_id = match outbound_socket_r.next().await {
            Some(Ok(message)) => match message {
                PoolMessageSC::ConnectAck(false, address, _worker_id) => { // todo address. TBD
                    error!("connect pool error, server rejected.");
                    return Ok(());
                }
                PoolMessageSC::ConnectAck(true, address, worker_id) => {
                    info!("connect pool success, my worker id: {:?}", worker_id);
                    worker_id.unwrap()
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

        loop {
            info!("tokio::select before io_message_process_loop"); //delete later todo
            tokio::select! {
                Some(request) = net_handler.recv() => {
                    match request {
                        NetRequest::Submit(job_id, address, prover_solution) => {
                            let message = PoolMessageCS::Submit(worker_id, job_id, address, prover_solution);
                            info!("NetRequest Submit"); //delete later todo
                            if let Err(error) = outboud_socket_w.send(message).await {
                                error!("[Submit to Pool Server] {}", error);
                            }
                        }
                        NetRequest::Exit => {
                            let message = PoolMessageCS::DisConnect(worker_id);
                            info!("NetRequest Exit"); //delete later todo
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
                                let request = ProverRequest::Notify(job_id, target, epoch_challenge);
                                info!("Notify from Pool Server");
                                if let Err(error) = prover_router.send(request).await {
                                    error!("[Notify from Pool Server] {}", error);
                                } else {
                                    debug!("Notify from Pool Server ok");
                                }
                            }
                            PoolMessageSC::ShutDown => {
                                info!("Pool server shutdown, reconnect...");
                                break;
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
        info!("Before Prover_router.send(ProverRequest::TerminateJob)"); //delete later todo
        // Lost link to pool， Terminate current Job
        let _ = prover_router.send(ProverRequest::TerminateJob).await;

        Ok(())
    }

    pub async fn start_pool_tcp_client<N: Network>(
        &self,
        candidate_pools: Vec<SocketAddr>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        //E::tasks().append(
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
                if Self::io_message_process_loop::<N, TcpStream>(custom_name.clone(), prover_router.clone(), &mut net_handler, stream)
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

    pub async fn reconnect_via_tcp(candidates_pool: Vec<SocketAddr>) -> Result<TcpStream> {
        for pool_ip in candidates_pool {
            match timeout(Duration::from_millis(5000), TcpStream::connect(pool_ip)).await {
                Ok(stream) => match stream {
                    Ok(stream) => {
                        debug!("connected to pool tcp://{}", pool_ip);
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
        candidate_pools: Vec<SocketAddr>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        //E::tasks().append(
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

    pub async fn reconnect_via_ssl(candidates_pool: Vec<SocketAddr>) -> Result<TlsStream<TcpStream>> {
        let mut root_cert_store = rustls::RootCertStore::empty();
        let mut cafile = BufReader::new(File::open("ca.crt")?);
        let certs = rustls_pemfile::certs(&mut cafile)?;
        let trust_anchors = certs.iter().map(|cert| {
            let ta = webpki::TrustAnchor::try_from_cert_der(&cert[..]).unwrap();
            OwnedTrustAnchor::from_subject_spki_name_constraints(ta.subject, ta.spki, ta.name_constraints)
        });
        root_cert_store.add_server_trust_anchors(trust_anchors);

        let config = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let domain = rustls::ServerName::try_from("sixpool").map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?;

        let connector = TlsConnector::from(Arc::new(config));
        for pool_ip in candidates_pool {
            match timeout(Duration::from_millis(5000), TcpStream::connect(pool_ip)).await {
                Ok(stream) => match stream {
                    Ok(stream) => {
                        debug!("tcp link accept by {}", pool_ip);
                        let stream = connector.connect(domain, stream).await?;
                        debug!("connected to pool ssl://{}", pool_ip);
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
                let _ = prover_router.send(ProverRequest::Exit).await;
                let _ = net_router.send(NetRequest::Exit).await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                info!("Exit gracefully");
                std::process::exit(0);
            }
            Err(error) => error!("tokio::signal::ctrl_c encountered an error: {}", error),
        }
    });
}

fn main() -> Result<()> {
    let worker = Worker::from_args();
    initialize_logger(worker.verbosity, None);
    info!("worker start.");
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
