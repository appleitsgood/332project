# Distributed Sort (Master–Worker)

https://www.miricanvas.com/login?redirect=%2Fv2%2Fdesign2%2Ffe8b0eff-3c3e-458b-9da8-2367a0eba885

This project implements a distributed sort with a Master/Worker architecture over gRPC (ScalaPB).
This README focuses on **how to build and run** the system (locally and over SSH).
* Note that fault tolerance is not yet implemented.

---

## Requirements

- Java 8+
- Scala **2.13**
- sbt **1.8+**
- TCP connectivity between master and workers (for cluster use)

---

## Build

From the project root:

```bash
sbt compile
```

### Thin JAR

```bash
sbt package
```

This produces:

```text
target/scala-2.13/332project_2.13-0.1.0-SNAPSHOT.jar
```

### Fat JAR (optional, if `sbt-assembly` is configured)

```bash
sbt assembly
```

Example output:

```text
target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar
```

---

## Running with sbt (Local)

You can run master and workers directly through sbt.

### Master (sbt)

```bash
sbt "runMain master.MasterMain <#workers>"
```

Example:

```bash
sbt "runMain master.MasterMain 3"
```

The master prints its listening address, e.g.:

```text
[MASTER] listening on 127.0.0.1:37035
```

You will use this `<masterHost:port>` when starting workers.

### Worker (sbt)

```bash
sbt "runMain worker.WorkerMain <masterHost:port> -I <inputDir> -O <outputDir>"
```

Example:

```bash
sbt "runMain worker.WorkerMain 127.0.0.1:37035 -I dataset/small -O out/small"
```

- `<inputDir>`: a directory or a single file.
- `<outputDir>`: output directory (sorted files `partition.N` will be written there).

---

## Running with Fat JAR (Assembly)

If you built an assembly JAR (via `sbt assembly`), you do not need to specify extra dependencies on the classpath.

### Master (assembly JAR)

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   master.MasterMain <#workers>
```

Example:

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   master.MasterMain 3
```

### Worker (assembly JAR)

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   worker.WorkerMain <masterHost:port> -I <inputDir> -O <outputDir>
```

Example:

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   worker.WorkerMain 127.0.0.1:37035 -I dataset/small -O out/small
```

---

## SSH / Cluster Usage

This section describes how to run the system on multiple machines via SSH (one master node and several worker nodes).

### 1. Deploy Code or JAR

You can either clone the repo on each node or build locally and copy the JAR.

#### Option A: Clone on Each Node

On each node (master and workers):

```bash
ssh <user>@<node>
git clone https://github.com/your-org/332project.git
cd 332project
sbt package    # or sbt assembly
```

#### Option B: Build Locally, Copy JAR

On your local machine:

```bash
sbt package    # or sbt assembly
```

Then copy to nodes:

```bash
scp target/scala-2.13/332project_2.13-0.1.0-SNAPSHOT.jar <user>@masterIP:~/332project/
scp target/scala-2.13/332project_2.13-0.1.0-SNAPSHOT.jar <user>@vm01:~/332project/
scp target/scala-2.13/332project_2.13-0.1.0-SNAPSHOT.jar <user>@vm02:~/332project/
# ...
```

If the cluster has a shared home (NFS), building/copying once is sufficient.

---

### 2. Prepare Input and Output Directories

Per worker, choose directories to use as input/output. Example layout:

```text
~/vm1/dataset/small     # on worker01
~/vm2/dataset/small     # on worker02
~/vm3/dataset/small     # on worker03
```

Outputs can be:

```text
~/vm1/out/small
~/vm2/out/small
~/vm3/out/small
```

Workers will write sorted `partition.N` files under their respective `out/...` directories.

---

### 3. Start Master on the Master Node

On the master node:

```bash
ssh <user>@master
cd ~/332project
```

Using assembly JAR (if you built one):

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   master.MasterMain <#workers>
```

The master prints its listening address, e.g.:

```text
[MASTER] listening on 192.168.X.Y:PORT
```

Workers must use this `<masterHost:port>`.

For long runs, you may want to start the master inside a `tmux` or `screen` session:

```bash
tmux new -s master
cd ~/332project
java -cp target/scala-2.13/332project_2.13-0.1.0-SNAPSHOT.jar master.MasterMain 3
```

---

### 4. Start Workers on Worker Nodes

On each worker node:

```bash
ssh <user>@worker01
cd ~/332project
```

Assembly JAR example:

```bash
java -jar target/scala-2.13/332project-assembly-0.1.0-SNAPSHOT.jar   worker.WorkerMain 192.168.X.Y:PORT   -I vm1/dataset/small   -O vm1/out/small
```

Repeat for `worker02`, `worker03`, etc., adjusting:

- Hostname (`worker02`, `worker03`, …)
- Input/output directories (`vm2/...`, `vm3/...`, …)
- The same `<masterHost:port>` printed by the master.


---

### 5. Completion

- Master prints logs as workers register and progress through the job.
- When all workers finish, the master logs that all merges are done and then exits.
- Final sorted output is in `partition.N` files under each worker’s output directory.
