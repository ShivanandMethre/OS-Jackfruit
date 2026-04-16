# Multi-Container Runtime with Kernel Memory Monitor

## Team Information
- **Name:** shivanand
  **SRN:** PES2UG24CS472

- **Name:** Shreenandan
  **SRN:** PES2UG24CS479

---

## Project Overview
This project implements a lightweight Linux container runtime in C with support for managing multiple containers through a centralized supervisor process. It also includes a kernel-space memory monitor that enforces soft and hard memory limits on container processes.

The system enables container lifecycle management, logging, inter-process communication, and scheduling experimentation within an isolated environment.

---

## System Architecture

### User-Space Runtime (engine.c)
- Implements a long-running supervisor process 
- Manages multiple containers concurrently 
- Handles container lifecycle (start, run, stop) 
- Maintains metadata for each container 
- Captures container output using a bounded-buffer logging system 
- Provides a CLI interface for interaction 
### Kernel-Space Monitor (monitor.c)
- Implemented as a Linux Kernel Module 
- Tracks container processes using their PIDs 
- Enforces memory usage policies 
- Differentiates between soft and hard memory limits 
- Communicates with user-space via `ioctl` 

---

## Features
- Multi-container management with a single supervisor 
- Process isolation using namespaces (PID, UTS, mount) 
- Filesystem isolation using separate root filesystems 
- CLI-based container control 
- Bounded-buffer logging with producer-consumer design 

### IPC Mechanisms
- Pipes for logging 
- Socket/FIFO/shared memory for CLI communication 

- Kernel-level memory monitoring 
- Scheduling experiments using container workloads 
- Clean resource management and teardown 

---

## Build and Execution

### Build Project
```bash
make
````

### Load Kernel Module

```bash
sudo insmod monitor.ko
```

### Verify Device

```bash
ls -l /dev/container_monitor
```

### Start Supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### Create Container Filesystems

```bash
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

### Start Containers

```bash
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96
```

### List Containers

```bash
sudo ./engine ps
```

### View Logs

```bash
sudo ./engine logs alpha
```

### Stop Containers

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

### Kernel Logs

```bash
dmesg | tail
```

### Unload Module

```bash
sudo rmmod monitor
```

---

## CLI Commands

```bash
engine supervisor <base-rootfs>
engine start <id> <container-rootfs> <command> [options]
engine run   <id> <container-rootfs> <command> [options]
engine ps
engine logs <id>
engine stop <id>
```

---

## Logging System

* Uses pipes to capture stdout and stderr from containers
* Implements a bounded buffer with producer-consumer threads

### Guarantees

* No data loss

* No deadlocks

* Safe concurrent access

* Logs are stored per container

---

## Memory Monitoring

* Tracks Resident Set Size (RSS) of container processes

* **Soft Limit:** Logs warning when exceeded

* **Hard Limit:** Terminates process when exceeded

* Uses kernel-space enforcement for reliability

---

## Scheduling Experiments

* Supports CPU-bound and I/O-bound workloads
* Allows configuration using `nice` values

### Observations

* CPU sharing
* Execution time differences
* Scheduler fairness and responsiveness

---

## Engineering Analysis

### Isolation Mechanisms

Containers are isolated using Linux namespaces (PID, UTS, mount) and separate root filesystems. Each container operates independently while sharing the host kernel.

### Supervisor and Lifecycle Management

A persistent supervisor process manages all containers, ensuring proper process tracking, signal handling, and cleanup of child processes.

### IPC and Synchronization

Two IPC mechanisms are used:

* Pipes for logging
* Socket/FIFO/shared memory for control

Synchronization is handled using mutexes and condition variables to avoid race conditions and ensure safe concurrent access.

### Memory Management

RSS is monitored to track actual memory usage. Soft and hard limits allow flexible control, with enforcement implemented in kernel space for accuracy and reliability.

### Scheduling Behavior

Experiments demonstrate how Linux scheduling policies affect process execution, highlighting trade-offs between fairness and performance.

---

## Design Decisions and Tradeoffs

| Component      | Decision            | Tradeoff                                |
| -------------- | ------------------- | --------------------------------------- |
| Isolation      | chroot + namespaces | Simpler but less secure than pivot_root |
| Supervisor     | Centralized daemon  | Single point of control                 |
| Logging        | Bounded buffer      | Added synchronization complexity        |
| IPC            | Separate channels   | Increased design complexity             |
| Kernel Monitor | Kernel enforcement  | Requires module handling                |

---

## Scheduler Experiment Results

* CPU-bound processes with higher priority complete faster
* I/O-bound processes remain responsive under load
* Linux scheduler balances fairness and throughput effectively

---

## Conclusion

This project demonstrates practical implementation of containerization concepts, kernel interaction, and operating system fundamentals including process management, memory control, synchronization, and scheduling.

```

---

### 🔧 Why your previous version broke
Your commit ignored indentation because:
- No `#` headings → everything looked like plain text  
- No `-` for lists → lines merged  
- No triple backticks → commands not formatted  
- Table not in Markdown format  

---

If you want, I can also:
- Add **badges (build, language, etc.)**
- Make it **more professional (top-tier GitHub style)**
- Or include **screenshots section formatting** (very important for marks)
```






















