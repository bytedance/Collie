# Traffic Engine


## Compile and setup:
- For GPU Direct RDMA, set the environment variable GDR=1 and check with the cuda path (in `./helper.hpp`).

    ```
    GDR=1 make -j8;
    ```
- For non-GDR version:

    ```
    make -j8;
    ```
- For DEBUG mode:

    ```
    DEBUG=1 make -j8
    ```
## Usage

- Use help messages to see the parameters. All params are in helper.cpp/hpp
```
    ./collie_engine --help; 
```
- Sample usage:

    Traffic engine helps generate complex traffic patterns for Collie's search. Here is a sample.

    To set up a server (192.168.0.1)
    - use **mlx5_0**
    - GID index is **3**
    - listen on port **3001** (TCP port) to use out-of-band (TCP) transmission for RDMA connection setup.
    - accept Reliable Connection (RC, **qp_type=2**)
    - support RDMA MTU of 1024 Bytes. (**MTU type = 3**)
    - owns **4** memory regions, each of MR contains **4** buffers, each buffer's size is **64 KB**.
    - keep posting receive request following this pattern: one receive request (WQE) has one scatter-gather element (SGE), each SGE points to one 64KB buffer; then, one WQE has two SGE, each SGE points to one 4KB buffer.

    Here is the command for server:
    ```
    ./collie_engine --server --dev=mlx5_0 --gid=3 --qp_type=2 --mtu=3 --qp_num=1 --buf_num=4 --mr_num=4 --buf_size=65536 --receive=1_65536,2_4096_4096
    ```

    Then, we set up a client (any IP address as long as can reach 192.168.0.1)
    - also use **mlx5_0** and GID index of **3**
    - connect to **192.168.0.1** 
    - use RC type of QP (**qp_type=2**)
    - MTU is set to 1024 Bytes for RDMA (**MTU type = 3**)
    - the memory organization (buf, mr) is the same as the server.
    - keep posting requests as the following pattern (repeatedly)
        - 1 SEND WQE, contains 1 SGE of 5120 Bytes.
        - 1 WRITE WQE, contains 2 SGE, one 4KB and one 512 Bytes.
        - 1 READ WQE, contains 1 SGE of 64 KB.   
    

    Here is the command for client:
    ```
    ./collie_engine --connect=192.168.0.1 --dev=mlx5_0 --gid=3 --qp_type=2 --mtu=3 --qp_num=1 --buf_num=4 --mr_num=4 --mr_size=65536 --request=s_1_5120,w_2_4096_512,r_1_65536
    ```


- Collie mainly tests between two endhosts while traffic engine support incast (many to one or one to many) communications. Here are important parameters for this use.
    - **--host_num**: the number of clients that a server need to serve. (Total number of QPs = host_num * qp_per_host)
    - **--connect**: multiple IPs or hostnames, split by ',' (e.g., --connect=host-01,host-02)

## Content
- `helper.hpp & helper.cpp` -- user parameter definition (gflags) and general assistant functions.

- `memory.hpp & memory.cpp` -- endhost's memory management. 

- `endpoint.hpp & endpoint.cpp` -- each endpoint is a wrapper for a queue pair (QP). State transition and verbs wrapper are implemented here.

- `context.hpp & context.cpp` -- a context is an instance of an endhost. Each context contains a memory pool and several endpoints. Datapath functions (how requests are generated) are implemented here.
