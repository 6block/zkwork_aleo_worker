# zkwork_aleo_worker
```shell
tcp_server=36.189.234.195:10003
ssl_server=36.189.234.195:10004
reward_address=aleo1xxx...
./zkwork_aleo_worker --address $reward_address --tcp_server $tcp_server --verbosity 1 --custom_name "myworker1"
```

## GPU Performance 

### zkwork_aleo_worker v0.4.2

We have tested `f3-prover 1.4.5` and `zkwork_aleo_worker v0.4.2` in same env,
- 1 * `AMD EPYC 7352 24-Core Processor`
- 1 * `NVIDIA GeForce RTX 3090`
- epoch 597
- epoch hash: `ab1ane307htly8lts6drnymq5v06jqjqkg9dgwjj8gnsak9mvzelu9qq8u7wr`

<table>
  <tr>
   <td><strong>Name & Version</strong>
   </td>
   <td><strong>Performance</strong>
   </td>
  </tr>
  <tr>
   <td>f3-prover 1.4.5
   </td>
   <td>962.73
   </td>
  </tr>
  <tr>
   <td>zkwork-aleo-worker v0.4.2
   </td>
   <td>1476.43
   </td>
  </tr>
</table>

### zkwork_aleo_worker v0.4.3

We have tested `h9 v2.0.1-4` and `zkwork_aleo_worker v0.4.3` in same env,
- 2 * `AMD EPYC 7543 32-Core Processor`
- 8 * `NVIDIA GeForce RTX 3080`
- epoch 792
- epoch hash: `ab1eqgz5t874l822e26h9egnz37kx85ufgw30u9w3urzzaas6qg55gqdtzhay`
  
<table>
  <tr>
   <td><strong>Name & Version</strong>
   </td>
   <td><strong>Performance</strong>
   </td>
  </tr>
  <tr>
   <td>h9 v2.0.1-4
   </td>
   <td>3383
   </td>
  </tr>
  <tr>
   <td>zkwork-aleo-worker v0.4.3
   </td>
   <td>4620
   </td>
  </tr>
</table>

## Download

- CPU prover: wget https://github.com/6block/zkwork_aleo_worker/releases/download/v0.4.2/zkwork-0.4.2-ub20-cpu.tar.gz
- GPU prover: wget https://github.com/6block/zkwork_aleo_worker/releases/download/v0.4.3/zkwork-0.4.3-ub20-gpu.tar.gz
- GPU prover: wget https://github.com/6block/zkwork_aleo_worker/releases/download/v0.4.3/zkwork-0.4.3-ub22-gpu.tar.gz

## Requirements

- OS version: Ubuntu 20.04 and Ubuntu 22.04

## Usage 

```shell
worker 0.4.3
The zk.work team <zk.work@6block.com>
0ca5f69

USAGE:
    zkwork_aleo_worker [FLAGS] [OPTIONS] --address <address>

FLAGS:
    -h, --help       Prints help information
        --ssl        If the flag is set, the worker will use ssl link
    -V, --version    Prints version information

OPTIONS:
        --address <address>              Specify this as a mining node, with the given aleo address
        --custom_name <custom-name>      Specify the custom name of this worker instance [default: sixworker]
        --parallel_num <parallel-num>    Specify the parallel number of process to solve coinbase_puzzle [default:
                                         2]
        --ssl_server <ssl-servers>...    Specify the pool(ssl) that the worker is contributing to
        --tcp_server <tcp-servers>...    Specify the pool(tcp) that the worker is contributing to
        --threads <threads>              Specify the threads per coinbase_puzzle solve process, defalut:1 [default:
                                         1]
        --verbosity <verbosity>          Specify the verbosity of the node [options: 0, 1, 2, 3] [default: 2]
        -g, --gpu_index <GPU_INDEXES>    Specify gpu index to solve puzzle, all gpus are used by default, eg. -g 0 -g 1 -g 2 ...
```

## License

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](./LICENSE.md)
