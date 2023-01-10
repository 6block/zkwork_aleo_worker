# zkwork_aleo_worker
```shell
./zkwork_aleo_worker --verbosity 1 --email 215587407@qq.com --tcp_server "abcd_test.com:10001" --ssl_server "abcd_test.com:10002" --custom_name miner_1 --parallel_num 12  --ssl
```
```shell
worker 0.3.3
The zk.work team <zk.work@6block.com>
aa51b4b

USAGE:
    zkwork_aleo_worker_gpu [FLAGS] [OPTIONS] --email <email>

FLAGS:
    -h, --help       Prints help information
        --ssl        If the flag is set, the worker will use ssl link
    -V, --version    Prints version information

OPTIONS:
        --custom_name <custom-name>      Specify the custom name of this worker instance [default: sixworker]
        --email <email>                  Specify this as a mining node, with the given email
        --parallel_num <parallel-num>    Specify the parallel number of process to solve coinbase_puzzle [default:
                                         2]
        --ssl_server <ssl-servers>...    Specify the pool(ssl) that the worker is contributing to
        --tcp_server <tcp-servers>...    Specify the pool(tcp) that the worker is contributing to
        --threads <threads>              Specify the threads per coinbase_puzzle solve process, defalut:16 [default:
                                         16]
        --verbosity <verbosity>          Specify the verbosity of the node [options: 0, 1, 2, 3] [default: 2]
```
try adjusting the following parameters for best performance，--parallel_num、--threads，“parallel 12 --threads 4“ is recommended. Threads can be adjusted based on different cpus.

# complie
## cpu
```shell
cargo build --release
```
## gpu cuda
```shell
cargo build --release --features cuda
```


## License

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](./LICENSE.md)
