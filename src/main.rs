// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

#[macro_use]
extern crate tracing;

use anyhow::{anyhow, Result};
use futures::SinkExt;
use rand::thread_rng;
use zkwork_aleo_protocol::{
    environment::{Environment, SixPoolWorkerTrial},
    node::initialize_logger,
    message::Data,
    poolmessage::{PoolMessageCS, PoolMessageSC},
};
use snarkvm::dpc::{posw::PoSWProof, prelude::*, testnet2::Testnet2};
use std::{
    convert::TryFrom,
    fs::File,
    io::{self, BufReader},
    marker::PhantomData,
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
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

type ProverRouter<N> = mpsc::Sender<ProverRequest<N>>;
type ProverHandler<N> = mpsc::Receiver<ProverRequest<N>>;
type NetRouter<N> = mpsc::Sender<NetRequest<N>>;
type NetHandler<N> = mpsc::Receiver<NetRequest<N>>;
#[derive(Debug)]
pub enum ProverRequest<N: Network> {
    WorkerJob(u64, BlockTemplate<N>),
    TerminateJob,
    Exit,
}

pub enum NetRequest<N: Network> {
    ShareBlock(Address<N>, N::PoSWNonce, N::BlockHash, Data<PoSWProof<N>>),
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
}

impl Worker {
    // Starts the worker.
    pub async fn start<N: Network, E: Environment>(self) -> Result<()> {
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
                self.start_prover_process::<N, E>(address, net_router.clone(), prover_handler)
                    .await?;

                if self.ssl {
                    self.start_pool_ssl_client::<N, E>(self.ssl_servers.clone(), prover_router.clone(), net_handler)
                        .await?;
                } else {
                    self.start_pool_tcp_client::<N, E>(self.tcp_servers.clone(), prover_router.clone(), net_handler)
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

    pub async fn start_prover_process<N: Network, E: Environment>(
        &self,
        address: Address<N>,
        net_router: NetRouter<N>,
        mut prover_handler: ProverHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        E::tasks().append(task::spawn(async move {
            let _ = router.send(());
            let mut terminator_previous = Arc::new(AtomicBool::new(false));
            loop {
                tokio::select! {
                    Some(request) = prover_handler.recv() => match request {
                        ProverRequest::WorkerJob(share_difficulty, block_template) => {
                            let target_difficulty = block_template.difficulty_target();
                            info!("WorkerJob share difficulty: {} difficulty target: {}", share_difficulty, target_difficulty);
                            let net_router = net_router.clone();
                            //Terminate previous job
                            terminator_previous.store(true, Ordering::SeqCst);
                            let terminator = Arc::new(AtomicBool::new(false));
                            terminator_previous = terminator.clone();
                            E::tasks().append(task::spawn(async move {
                                loop {
                                    let terminator_clone = terminator.clone();
                                    let block_height = block_template.block_height();
                                    let block_template = block_template.clone();
                                    let previous_block_hash = block_template.previous_block_hash();
                                    if terminator.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    let result = task::spawn_blocking(move || {
                                        E::thread_pool().install(move || {
                                            loop {
                                                let block_header =
                                                    BlockHeader::mine_once_unchecked(&block_template, &terminator_clone, &mut thread_rng())?;
                                                // Ensure the share difficulty target is met.
                                                if N::posw().verify(
                                                    block_header.height(),
                                                    share_difficulty,
                                                    &[*block_header.to_header_root().unwrap(), *block_header.nonce()],
                                                    block_header.proof(),
                                                ) {
                                                    return Ok::<(N::PoSWNonce, PoSWProof<N>, u64), anyhow::Error>((
                                                        block_header.nonce(),
                                                        block_header.proof().clone(),
                                                        block_header.proof().to_proof_difficulty()?,
                                                    ));
                                                }
                                            }
                                        })
                                    })
                                    .await;
                                    if terminator.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    match result {
                                        Ok(Ok((nonce, proof, proof_difficulty))) => {
                                            debug!(
                                                "Prover successfully mined a share for unconfirmed block {} with proof difficulty of {}",
                                                block_height, proof_difficulty
                                            );
                                            if proof_difficulty <= target_difficulty {
                                                info!("Mined an Candidate block {} with proof difficulty {} target difficulty {}",
                                                    block_height, proof_difficulty, target_difficulty
                                                );
                                            }

                                            // Send a `` to the operator.
                                            let message = NetRequest::ShareBlock(address, nonce, previous_block_hash, Data::Object(proof));
                                            if let Err(error) = net_router.send(message).await {
                                                error!("[ShareBlock] {}", error);
                                            }
                                        }
                                        Ok(Err(error)) => error!("{}", error),
                                        Err(error) => error!("{}", anyhow!("Failed to mine the next block {}", error)),
                                    }
                                }
                            }));
                        }
                        ProverRequest::TerminateJob => terminator_previous.store(true, Ordering::SeqCst),
                        ProverRequest::Exit => return,
                    }
                }
            }
        }));
        let _ = handler.await;
        Ok(())
    }

    pub async fn io_message_process_loop<N: Network, E: Environment, T: AsyncRead + AsyncWrite>(
        custom_name: String,
        prover_router: ProverRouter<N>,
        net_handler: &mut NetHandler<N>,
        stream: T,
    ) -> Result<()> {
        let (r, w) = split(stream);
        let mut outboud_socket_w = FramedWrite::new(w, PoolMessageCS::Unused::<N, E>(PhantomData));
        let mut outbound_socket_r = FramedRead::new(r, PoolMessageSC::Unused::<N, E>(PhantomData));
        let message = PoolMessageCS::Connect(0, 1, 0, 0, custom_name);
        if let Err(error) = outboud_socket_w.send(message).await {
            error!("[Connect pool] {}", error);
            return Ok(());
        }
        let worker_id = match outbound_socket_r.next().await {
            Some(Ok(message)) => match message {
                PoolMessageSC::ConnectResponse(false, _worker_id) => {
                    error!("connect pool error, server rejected.");
                    return Ok(());
                }
                PoolMessageSC::ConnectResponse(true, worker_id) => {
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
            tokio::select! {
                Some(request) = net_handler.recv() => {
                    match request {
                        NetRequest::ShareBlock(address, nonce, previous_block_hash, proof) => {
                            let message = PoolMessageCS::ShareBlock(worker_id, address, nonce, previous_block_hash, proof);
                            if let Err(error) = outboud_socket_w.send(message).await {
                                error!("[ShareBlock] {}", error);
                            }
                        }
                        NetRequest::Exit => {
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
                            PoolMessageSC::WorkerJob(share_difficulty, block_template) => {
                                if let Ok(block_template) = block_template.deserialize().await {
                                    let request = ProverRequest::WorkerJob(share_difficulty, block_template);
                                    if let Err(error) = prover_router.send(request).await {
                                        error!("[WOrkerJob] {}", error);
                                    }
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

        // Lost link to pool， Terminate current Job
        let _ = prover_router.send(ProverRequest::TerminateJob).await;

        Ok(())
    }

    pub async fn start_pool_tcp_client<N: Network, E: Environment>(
        &self,
        candidate_pools: Vec<SocketAddr>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        E::tasks().append(task::spawn(async move {
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
                if Self::io_message_process_loop::<N, E, TcpStream>(custom_name.clone(), prover_router.clone(), &mut net_handler, stream)
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }));
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

    pub async fn start_pool_ssl_client<N: Network, E: Environment>(
        &self,
        candidate_pools: Vec<SocketAddr>,
        prover_router: ProverRouter<N>,
        mut net_handler: NetHandler<N>,
    ) -> Result<()> {
        let (router, handler) = oneshot::channel();
        let custom_name = self.custom_name.clone();
        E::tasks().append(task::spawn(async move {
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
                if Self::io_message_process_loop::<N, E, TlsStream<TcpStream>>(
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
        }));
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
    let (num_tokio_worker_threads, max_tokio_blocking_threads) = (num_cpus::get(), 512); // 512 is tokio's current default

    // Initialize the runtime configuration.
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .worker_threads(num_tokio_worker_threads)
        .max_blocking_threads(max_tokio_blocking_threads)
        .build()?;

    let num_rayon_cores_global = num_cpus::get();

    // Initialize the parallelization parameters.
    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .num_threads(num_rayon_cores_global)
        .build_global()
        .unwrap();

    runtime.block_on(async move {
        worker
            .start::<Testnet2, SixPoolWorkerTrial<Testnet2>>()
            .await
            .expect("Failed to start the worker");
    });
    Ok(())
}
