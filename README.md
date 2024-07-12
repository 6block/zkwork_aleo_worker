# zkwork_aleo_worker
```shell
tcp_server=36.189.234.195:10003
ssl_server=36.189.234.195:10004
reward_address=aleo1xxx...
./zkwork_aleo_worker --address $reward_address --tcp_server $tcp_server --verbosity 1 --custom_name "myworker1"
```
```shell
worker 0.4.2
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
```

# complie
```shell
cargo build --release
```


## License

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](./LICENSE.md)
