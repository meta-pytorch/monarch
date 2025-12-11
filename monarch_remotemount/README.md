## TL;DR What is remotemount?

In your client script, you can run


```
remotemount(host_mesh, sourcepath, mntpoint=sourcepath)
```


Now the files at sourcepath on the client will appear as a read-only mounted file system at mntpoint on every host in the host_mesh. The files are distributed via RDMA with a monarch + pyfuse implementation that is only around 400 lines of code.

This single API has a ton of very powerful uses that speed AI research:



* Immediately sync all your local python files
* Ability to use the client's virtual environments / conda install remotely
* A distribution system for pypi wheels (point pip at a directory of packages).
* Scale this distribution as the job grows bigger with minimal implementation changes.

We collected evidence that we use to project that with beefy machines, with lots of RAM, modern local SSDs and blazingly fast networks, we can cold start a new job in three minutes rather than 10s of minutes.

We assume both CPU RAM and local SSD space are not the bottleneck for iterative AI research. There are mitigations to this if this assumption is wrong.


## Context and motivation

Launching a remote job to run your local experiment can take a very long time.

There are typically a few components

1) Scheduler overhead (e.g. Slurm or MAST): allocate and maybe even image a pool of remote hosts

2) Code distribution overhead (e.g. fbpkg or NFS (e.g. AWS fsx))

3) (optional) Checkpoint distribution overhead (e.g. a ViT backbone or 8B LLM encoder)

4) Python library import overhead (e.g. import torch)

5) Model initialization and/or checkpoint overhead (e.g. torch.load(checkpoint.pth))

We tackle 2, 3 and to a lesser extent 4 and 5 in this post.


## How does it work?


```
remotemount(host_mesh, sourcepath, mntpoint=sourcepath)
```


remotemount creates an in-memory copy of sourcepath. It then allocates a process pool on the given host_mesh. It transmits the data within sourcepath and makes it available under mntpoint on each host. In particular, remotemount reads all of the files under sourcepath into a single in-memory buffer and then sends it in chunks to the remote hosts along with a single python dictionary of path metadata.

The user can then run arbitrary commands or code that uses the data found locally under mntpoint on each host in the Monarch host_mesh provided by the user.


## Alternatives

One viable alternative would be to use a filesystem such as squashfs and distribute the image files across the network directly. The result can then be mounted on each host using e.g. squashfuse. If compression is turned off this can be pretty quick to build (see Benchmark section). Regardless, by writing the filesystem from scratch in FUSE, which is only roughly 300 lines of code, we can mix operations such as remote reads or appending update information with filesystem operations.


## Example usage

Using `remotemount` we can build a little tool run.py that lets us run bash scripts remotely.


```
class BashActor(Actor):

    @endpoint
    def run(self, script: str):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=True) as f:
            f.write(script)
            f.flush()
            result = subprocess.run(['bash', f.name], capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr


def main(source_dir: str,
         script: str):

    mount_point = source_dir
    # Create a slurm job with 2 hosts
    slurm_job = SlurmJob({"mesh1": 2}, [...])
    host_mesh = slurm_job.state().mesh1
    procs = host_mesh.spawn_procs()

    with open(Path(script).resolve()) as f:
        script = f.read()

    with remotemount(host_mesh, str(source_dir), str(mount_point)):
        bash_actors = procs.spawn("BashActor", BashActor)
        result = bash_actors.run.call(script).get()
        # Print stdout for each host in order
        print("\n".join([f"== host{i} ==\n{r[1]['stdout']}" 
                  for i, r in enumerate(results)]))
```


We can then create virtual environments that we can use within the bash scripts to run various commands.


### Code distribution: remote importing

One of the most basic examples is creating a python virtual environment and remotely importing a local package.

This is how we setup the environment


```
$ python -m venv /scratch/cpuhrsch/venv_torch
$ source /scratch/cpuhrsch/venv_torch/bin/activate
$ pip install torch
```


And this is the script we plan to run. Just take the mean of some random CUDA Tensor and print it.


```
$ cat examples/run_import.sh
#!/bin/bash

set -ex

source /scratch/cpuhrsch/venv_torch/bin/activate
cd /scratch/cpuhrsch/venv
python -c "import torch; print(torch.randn(123).cuda().mean())"
```


Let's give it a go.


```
time python run.py /scratch/cpuhrsch/venv_torch examples/run_import.sh
Found cached job at path: .monarch/job_state.pkl
Connecting to cached job
SLURM job 2347308 is running on 2 nodes: ['h100-000-126', 'h100-015-132']
Monarch internal logs are being written to /tmp/cpuhrsch/monarch_log.log
== host0 ==
tensor(0.1127, device='cuda:0')

== host1 ==
tensor(-0.1669, device='cuda:0')


real    0m54.254s
user    0m12.189s
sys     0m20.441s
```


Note in particular that our script reconnected to already existing jobs. So this time hides job allocation. Note that this time, 54s, includes the transfer of the entire venv, which includes a CUDA installation of torch. It's also a new environment into which we just installed torch.

Under next steps we detail future work that'll allow us to quickly append a chunk of data to very quickly add a new package or update a small file. This should allow us to rerun a job with small changes in just a few seconds.

Of course running locally is still quite a bit snappier


```
 time ./examples/run_import.sh 2> /dev/null
tensor(-0.0817, device='cuda:0')

real    0m2.285s
user    0m1.005s
sys     0m1.150s
```



### Checkpoint distribution: running a HuggingFace transformer on each host

We can run something a bit more substantial.


```
$ cat hf_example.py
import torch
from transformers import pipeline

model_id = "microsoft/Phi-3-mini-4k-instruct"
pipe = pipeline(
    "text-generation",
    model=model_id,
    [...]
)
messages = [
    {"role": "user", "content": "Explain the concept of recursion to a 5-year-old."},
]

outputs = pipe(
    messages,
    max_new_tokens=256,
    do_sample=True,
    temperature=0.7,
)
[...] # Print outputs in a nicely formatted way
```


After copying this file into our venv this can be wrapped into a simple shell script and executed remotely.

We create a new venv called "venv", activate it and pip install the various dependencies needed. 

We first run it locally once to download the checkpoint into the venv as well to make sure everything is self-contained and available without needing access to the internet on each remote host.


```
cat examples/run_hf_example.sh
#!/bin/bash

set -ex

source /scratch/cpuhrsch/venv/bin/activate
cd /scratch/cpuhrsch/venv
HF_HOME=/scratch/cpuhrsch/venv/hf_cache python hf_example.py
```



```
$ time ./examples/run_hf_example.sh 2> /dev/null
Downloading and loading microsoft/Phi-3-mini-4k-instruct...

Generating response...
--------------------------------------------------
Prompt: Explain the concept of recursion to a 5-year-old.
--------------------------------------------------
Response:
 Alright, imagine you're playing with a bunch of toy blocks, and you start stacking them up. Now, recursion is like when you keep stacking one block on top of another, but after stacking each block, you repeat the action of stacking another block on top, again and again. Just like when you build a tower, you keep adding one block at a time, that's a little bit like recursion, where a task is repeated over and over, each time doing slightly different work, just like adding a block to your tower.
--------------------------------------------------

real    0m9.381s
user    0m9.172s
sys     0m1.974s
```


and now run it remotely


```
$ time python run.py /scratch/cpuhrsch/venv examples/run_hf_example.sh
Found cached job at path: .monarch/job_state.pkl
Connecting to cached job
SLURM job 2347308 is running on 2 nodes: ['h100-000-126', 'h100-015-132']
Monarch internal logs are being written to /tmp/cpuhrsch/monarch_log.log
== host0 ==
Downloading and loading microsoft/Phi-3-mini-4k-instruct...

Generating response...
--------------------------------------------------
Prompt: Explain the concept of recursion to a 5-year-old.
--------------------------------------------------
Response:
 Okay, kiddo, imagine you are playing with your toy blocks. Now, if you have a tower of blocks and you want to make it twice as tall, what would you do? You would stack another tower of blocks on top, right?


This is kind of like what recursion in computer science is. It's like a toy block tower, but in computer programming. Instead of blocks, we have functions, and instead of making the tower taller, we make a problem simpler by breaking it into smaller parts that are easier to solve.


Think about when you play a game that makes you repeat a step over and over, like jumping. Each time you jump, it's the same as the last - that's recursive! You're repeating the same action until you reach the end of the game.


In computer programming, we write a function (our game) that does a certain task, and this function can call itself to do that same task again, but with a bit smaller of a problem. It keeps doing this until the problem is so small it can be solved easily - like reaching the goal of the game.


So, recursion is just a fancy way of saying,
--------------------------------------------------

== host1 ==
Downloading and loading microsoft/Phi-3-mini-4k-instruct...

Generating response...
--------------------------------------------------
Prompt: Explain the concept of recursion to a 5-year-old.
--------------------------------------------------
Response:
 Imagine you have a magical box that can copy itself. You put something inside the box, and then the box makes another box just like it. Now you have two boxes, and each box has a copy of itself inside. Every time the boxes copy themselves, it's like a fun game of magic that repeats. That's what recursion is – a process that keeps on making copies of itself until it reaches a point where it can't copy anymore, and then it stops. It's like a story that tells itself again and again, until it finishes telling the story.
--------------------------------------------------


real    2m35.504s
user    0m24.571s
sys     0m56.698s
```


This clearly took a lot more time than running it locally. We address this in the Benchmark results section.


### Scaling pip: running pip on multiple hosts without downloading from the internet

We don't need to constrain ourselves to PyTorch scripts here. For something else, let's use our mount to install a bunch of pypi wheels on new virtual environments within each host.

This might be useful if one tries to spin up a lot of different environments on multiple hosts.

By pulling wheels from a local cache folder we avoid overwhelming the pypi servers or just getting quota'd.

Let's download the packages we care about on our local host and then use those to install them remotely.


```
pip download -d /tmp/flat_wheels torch transformers sentencepiece
```


Now let's write a little script within which we install these packages in a new venv.


```
$ cat examples/run_cached_pip.sh
#!/bin/bash

set -ex

python -m venv /tmp/myvenv
source /tmp/myvenv/bin/activate
pip install --no-index --find-links /tmp/flat_wheels torch transformers sentencepiece
```


This runs wonderfully and seems to install the packages


```
python run.py /tmp/flat_wheels examples/run_cached_pip.sh --verbose
Found cached job at path: .monarch/job_state.pkl
Connecting to cached job
SLURM job 2347974 is running on 2 nodes: ['h100-000-126', 'h100-011-158']
Monarch internal logs are being written to /tmp/cpuhrsch/monarch_log.log
== host0 ==
[...]
Successfully installed MarkupSafe-3.0.3 certifi-2025.11.12 charset_normalizer-3.4.4 filelock-3.20.0 fsspec-2025.12.0 hf-xet-1.2.0 huggingface-hub-0.36.0 idna-3.11 jinja2-3.1.6 mpmath-1.3.0 networkx-3.4.2 numpy-2.2.6 nvidia-cublas-cu12-12.8.4.1 nvidia-cuda-cupti-cu12-12.8.90 nvidia-cuda-nvrtc-cu12-12.
8.93 nvidia-cuda-runtime-cu12-12.8.90 nvidia-cudnn-cu12-9.10.2.21 nvidia-cufft-cu12-11.3.3.83 nvidia-cufile-cu12-1.13.1.3 nvidia-curand-cu12-10.3.9.90 nvidia-cusolver-cu12-11.7.3.90 nvidia-cusparse-cu12-12.5.8.93 nvidia-cusparselt-cu12-0.7.1 nvidia-nccl-cu12-2.27.5 nvidia-nvjitlink-cu12-12.8.93 nvidi
a-nvshmem-cu12-3.3.20 nvidia-nvtx-cu12-12.8.90 packaging-25.0 pyyaml-6.0.3 regex-2025.11.3 requests-2.32.5 safetensors-0.7.0 sentencepiece-0.2.1 sympy-1.14.0 tokenizers-0.22.1 torch-2.9.1 tqdm-4.67.1 transformers-4.57.3 triton-3.5.1 typing-extensions-4.15.0 urllib3-2.6.1

== host1 ==
[...]
Successfully installed MarkupSafe-3.0.3 certifi-2025.11.12 charset_normalizer-3.4.4 filelock-3.20.0 fsspec-2025.12.0 hf-xet-1.2.0 huggingface-hub-0.36.0 idna-3.11 jinja2-3.1.6 mpmath-1.3.0 networkx-3.4.2 numpy-2.2.6 nvidia-cublas-cu12-12.8.4.1 nvidia-cuda-cupti-cu12-12.8.90 nvidia-cuda-nvrtc-cu12-12.8.93 nvidia-cuda-runtime-cu12-12.8.90 nvidia-cudnn-cu12-9.10.2.21 nvidia-cufft-cu12-11.3.3.83 nvidia-cufile-cu12-1.13.1.3 nvidia-curand-cu12-10.3.9.90 nvidia-cusolver-cu12-11.7.3.90 nvidia-cusparse-cu12-12.5.8.93 nvidia-cusparselt-cu12-0.7.1 nvidia-nccl-cu12-2.27.5 nvidia-nvjitlink-cu12-12.8.93 nvidia-nvshmem-cu12-3.3.20 nvidia-nvtx-cu12-12.8.90 packaging-25.0 pyyaml-6.0.3 regex-2025.11.3 requests-2.32.5 safetensors-0.7.0 sentencepiece-0.2.1 sympy-1.14.0 tokenizers-0.22.1 torch-2.9.1 tqdm-4.67.1 transformers-4.57.3 triton-3.5.1 typing-extensions-4.15.0 urllib3-2.6.1
```


If we're running within a docker image or other containerization solution we could even attempt to overwrite the default cache dir on the remote host. We can't mount at a point without an empty directory and a quick check reveals that the remote hosts don't have an empty cache directory.


```
echo "ls $(pip cache dir)/wheels" | python run.py /scratch/empty stdin
Found cached job at path: .monarch/job_state.pkl
Connecting to cached job
SLURM job 2347974 is running on 2 nodes: ['h100-000-126', 'h100-011-158']
Monarch internal logs are being written to /tmp/cpuhrsch/monarch_log.log
== host0 ==
0b
1f
7c
b3
bf
c0
c1
ec
ed

== host1 ==
0b
1f
7c
b3
bf
c0
c1
ec
ed
```



### Running an image remotely using apptainer

By now we have probably noticed that one downside of persistently running remote workers is a lack of containerization across reconnects. While the scheduler might containerize, we'd also like to containerize our commands within a worker to avoid statefulness across reconnects.

apptainer seems easily available on the fair-sc slurm cluster and is able to containerize, so let's give it a go.

In particular, we'll create a container that doesn't have access to the internet and connect the default pip cache folder to a host-level mount using remotemount.

Our goal here is to transparently redirect `uv pip install` commands to only install wheels available in a local cache that we make available via remotemount.

This is our image definition


```
# img.def
Bootstrap: docker
From: python:3.11-slim

%files
    ./packages /packages
    ./pip.conf /opt/pip.conf

%post
    pip install uv
    mv /opt/pip.conf /etc/pip.conf

%environment
    export UV_OFFLINE=1
    export UV_FIND_LINKS=/packages
    export UV_SYSTEM_PYTHON=1
    export UV_LINK_MODE=copy
```


And we download the following packages (for python 3.11) ahead of time using the following command


```
pip download -d ./packages --python-version 3.11 --only-binary=:all: requests numpy pandas uv
```


This is our pip configuration to redirect appropriately


```
# pip.conf
[global]
no-index = true
find-links = /packages
```


In a nutshell, we copy the packages into the apptainer image at build time and use environment variables to existing vanilla `uv pip install` commands. A next step here is to try to connect the local file system on the remote hosts so we avoid bundling the packages explicitly with the image.

We'll want to run with --containall and --network none to show that no other system resources were used and need to create an overlay fs to install the packages into.

We'll run the following script on remote hosts


```
apptainer overlay create --size 2048 /tmp/overlay.img # Create a 1GB overlay local to the remote host
mkdir /tmp/apptainer-work # Create a workdir
apptainer exec --containall --network none --workdir /tmp/apptainer-work --overlay /tmp/overlay.img /tmp/myapp/img.sif uv pip install requests numpy pandas
apptainer exec --containall --network none --workdir /tmp/apptainer-work --overlay /tmp/overlay.img /tmp/myapp/img.sif python -c "import pandas; print(pandas.__version__)"
```


The overlay image will make sure installs are persistent. We could also ship the overlay image, but the point here is to show how to redirect pip install commands to read from a local cache directory. In particular, the user could choose to install a different subset on each host, etc. as long as the cache is complete enough. The user could also use different images, i.e. different operating systems on various hosts.

apptainer itself might have its own performance characteristics when it comes to file reads from local disk. It's not clear that this is the best containerization solution for the job.

Here some output from the remote hosts


```
== host0 stdout ==
2.3.3

== host1 stdout ==
2.3.3

== host0 stderr ==
[...]
Using Python 3.11.14 environment at: /usr/local
Resolved 11 packages in 57ms
Prepared 11 packages in 144ms
Installed 11 packages in 5.12s
 + certifi==2025.11.12
[...]
INFO:    Setting --net (required by --network)

== host1 stderr ==
[...]
Using Python 3.11.14 environment at: /usr/local
Resolved 11 packages in 56ms
Prepared 11 packages in 129ms
Installed 11 packages in 4.09s
 + certifi==2025.11.12
[..]
INFO:    Setting --net (required by --network)
```



## Benchmark results and back-of-the-envelope roofline

Let's rerun our HuggingFace transformer example from above with more verbose logging and take a look at some of the details.


```
python run.py /scratch/cpuhrsch/venv examples/run_hf_example.sh --verbose
Found cached job at path: .monarch/job_state.pkl
Connecting to cached job
SLURM job 2357247 is running on 2 nodes: ['h100-000-126', 'h100-005-031']
Monarch internal logs are being written to /tmp/cpuhrsch/monarch_log.log
01:04:09 | INFO | [actor=<root>] Packing '/scratch/cpuhrsch/venv' into 8192MiB chunks...
01:04:09 | INFO | [actor=<root>] Creating metadata dictionary
01:04:10 | INFO | [actor=<root>] Concat file contents into a single buffer
01:04:29 | INFO | [actor=<root>] Creating chunks
01:04:29 | INFO | [actor=<root>] Packed into 3 bytearrays.
01:04:29 | INFO | [actor=<root>] Chunk sizes: [8192, 8192, 5139]MiB
01:04:31 | INFO | [actor=<root>] Sending chunks and remotely mounting /scratch/cpuhrsch/venv under /scratch/cpuhrsch/venv
01:04:31 | INFO | [actor=<root>] Sending meta
01:04:31 | INFO | [actor=<root>] Sending chunk of size 8192MiB
01:05:14 | INFO | [actor=<root>] Sending chunk of size 8192MiB
01:05:56 | INFO | [actor=<root>] Sending chunk of size 5139MiB
01:06:23 | INFO | [actor=<root>] Starting remote mount points
== host0 stdout ==
Downloading and loading microsoft/Phi-3-mini-4k-instruct...

Generating response...
--------------------------------------------------
Prompt: Explain the concept of recursion to a 5-year-old.
--------------------------------------------------
Response:
 Hey there! Imagine you have a toy that can play a fun game all by itself. When you give it a start, it tells the toy to play the game again, but this time with a small part of the toy
. It keeps doing that until the toy is just a little piece that can't play anymore. Each time, it looks back at how it started and remembers. That's like recursion! It's a fancy word f
or a toy, or a computer, telling itself to do something over and over, each time with a little bit less to do, until it's all done and remembers the fun it had.
--------------------------------------------------

== host1 stdout ==
Downloading and loading microsoft/Phi-3-mini-4k-instruct...

Generating response...
--------------------------------------------------
Prompt: Explain the concept of recursion to a 5-year-old.
--------------------------------------------------
Response:
 Imagine you have a magical box that can fit a smaller box inside it. Each time you put the smaller box inside, the smaller box also has a smaller box inside it, and it keeps going lik
e a little nesting doll. Now, recursion is like that. It's when a problem in math or computer science is solved by breaking it into smaller versions of the same problem, just like the
smaller dolls within the dolls.
--------------------------------------------------
```


Here is evidence to why we can project it to be seconds instead of minutes.

There are two key slow operations and two analogous computations we can perform to get a sense for how quick they could run.

We'll enumerate each stage of this scheduling and execution process and provide a back of the envelope calculation for a rough roofline.


### 1) Reading a directory into memory into a single bytearray ("Concat file contents into a single buffer")


```
$ du -sh /scratch/cpuhrsch/venv
14G     /scratch/cpuhrsch/venv

$ time find /scratch/cpuhrsch/venv -type f -exec cat {} + > /scratch/cpuhrsch/bigfile

real    0m5.818s
user    0m0.138s
sys     0m5.673s
```


I did not flush diskcache before this operation (I don't have sudo on the host). However, we could make the assumption that a user would use the same local environment remotely as they are actively using locally and thus it will have been heavily cached.

squashfs can use all 192 CPU threads and, without compression, is rather quick to build as well.


```
$ time mksquashfs /scratch/cpuhrsch/venv /tmp/output.img -noI -noD -noF -noX
[...]

real    0m4.790s
user    0m3.291s
sys     0m11.852s
```


We can read 14GByte this way in about 6 second, so let's say we can read 100GByte in less than 45 seconds.


### 2) Using infiniband ("Sending chunk of size [...]MiB")

Since Monarch currently doesn't support AWS EFA, we'll need to run these experiments on either CoreWeave or MAST to use infiniband (or add support for AWS EFA).

Luckily we have preliminary data from [prior studies](https://fb.workplace.com/groups/1268065457862874/permalink/1339654217370664/). Looking at the results here I think we can conclude that we'll be able to broadcast data from one host to 511 others with at least 200Gbit/s assuming we distribute directories on the order of 10s of GiB.

This is by far the most crucial assumption. We assume that we can distribute (broadcast) 10s of GBs at rates of 100s of Gbits. The study above does not measure the rate of an explicit broadcast operation.

Just to be a bit more conservative, let's assume we only have 100Gbit of effective bandwidth and we use a tree based distribution algorithm.

For 1024 hosts that'd be 10 steps at 100Gbit each. This would mean 10 transfers at 100Gbit to reach all hosts or 10x the amount of time of a single transfer. That would then leave us with 80s to transfer 100GByte of data to 1024 hosts.

However, real word broadcasting implementations are far more sophisticated and faster than this naive calculation. For example, breaking up the 100GByte of data into smaller chunks would allow us to avoid idle interconnects while far away hosts are being reached. Also note that we only need to distribute the data to the host, not to the individual GPUs. As the number of GPUs per host grows, 1024 hosts becomes a much more realistic count.

Let's conclude this section by saying that we could realistically transfer these 100Gbyte in 90 seconds. This is likely a very conservative estimate, but seems more comfortable before we have more experimental data or more benchmarks specific to broadcasting.


### 3) FUSE overhead ("Starting remote mount points")

We're now left with overhead from reading through a FUSE mount. We can write a small script to randomly read files from the local disk and through the mount point to show the difference.


```
$ cat examples/run_disk_time.sh
#!/bin/bash

find /scratch/cpuhrsch/venv -type f > /tmp/all_files
python -c "import random; random.seed(123); import sys; lines = sys.stdin.read().split('\n')[:-1]; random.shuffle(lines); print('\n'.join(lines))" < /tmp/all_files > /tmp/all_files_shuf

time xargs -d '\n' cat < /tmp/all_files_shuf > /tmp/bigfile
```


Locally this produces a total time of 6 seconds while when run remotely it produces a total time of 24 seconds. This means we either see a drop in latency or throughput when reading through the mount point.

To disambiguate this, let's read a large file (similar to a model checkpoint) and compare the times.

Let's create a 10GiB file using `dd if=/dev/zero of=myfile.img bs=1M count=10240` and read it via `time cat /scratch/cpuhrsch/myfiledir/myfile.img > /dev/null`

This takes about 1s to run locally and 4s to run remotely. So we do see about a 4x reduction in throughput when using this FUSE mount.

Note that this matters in particular for processes that are entirely dominated by disk read speeds. Let's take as an example importing torch. We find that locally this takes about 1.5s while remotely it takes about 2s.

I am again not flushing any disk caches here or otherwise taking care to avoid cache effects. We could further increase the precision of the instruments we use to gather this data, but are currently only collecting preliminary data to support a hypothesis and determine whether further investment is justified. Not flushing local host disk caches is likely to make the situation seem worse than it is, since the remote workers are reading from a mount that was just created and thus presumably not part of the disk cache yet.

Right now our FUSE mount is implemented using [fusepy](https://github.com/fusepy/fusepy), which is a Python based implementation. We are likely to increase performance by using rust or C or just by optimizing the Python code to, for example, avoid unnecessary in-memory copies when returning file bytes. We might also want to investigate new kernel features such as [FUSE Passthrough](https://docs.kernel.org/next/filesystems/fuse-passthrough.html), which [seems promising](https://fb.workplace.com/groups/1098625220606088/permalink/1850546958747240/) and can be used for a true zero-copy setup when reading data.

We'll also need to measure how this mount performs when accessed by a lot of concurrent processes. If a host has a lot of GPUs we should expect significant stress.

This still clearly needs to be addressed or alternatives such as squashfs or locally mounted disk images should be investigated. We should expect to be able to match or exceed disk speeds given that we store everything in RAM, but modern SSDs on AI clusters are very optimized for disk to GPU transfers and might be a better choice than RAM.


### 4) RAM Usage

Indeed the whole directory will be in memory. The machine I'm writing this report on has 2TiB (Amazon P5D). We need one mount per host.

If RAM usage becomes a bottleneck, we could also store the files on a local disk. The machine I'm writing this on has 28T of SSD space.

We assume both CPU RAM and local SSD space are not the bottleneck for iterative AI research.

However, we could ostensibly store the entire data on a local disk after transmission. This will add additional time to the startup. Further work is needed to investigate inifiband based RDMA transmission from SSD to SSD, which might avoid that downside.


### 5) Conclusion

Now, assuming we can read 100GB of data in 45s and can distribute it with 100Gbit/s in 90s, we should be able to broadcast a folder of size 100GB in less than 3 minutes.

Let's revisit our job components from the context section.

1) Scheduler overhead (e.g. Slurm or MAST): allocate and maybe even image a pool of remote hosts



* This should be close to zero given that the workers have already been allocated and are able to accept commands

2) Code distribution overhead (e.g. fbpkg or NFS (e.g. AWS fsx))

3) (optional) Checkpoint distribution overhead (e.g. a ViT backbone or 8B LLM encoder)



* We determined 2 and 3 to be less than 3 minutes if we can assume that it doesn't exceed 100GB.

4) Python library import overhead (e.g. import torch)



* Assuming we can provide a FUSE implementation that matches local SSD disk reads, this should remain the same.

5) Model initialization and/or checkpoint overhead (e.g. torch.load(checkpoint.pth))



* Assuming we can provide a FUSE implementation that matches local SSD disk reads, this should remain the same.
* This is a big assumption, because we'll need to also support memory mapped files for torch.load's popular mmap feature, which [FUSE passthrough does support](https://docs.kernel.org/next/filesystems/fuse-passthrough.html).

At the moment this still comes at reduced throughput at the mount level, but we should be able to reasonably mitigate this by optimizing the implementation.

Overall this indicates we can launch a job in less than 3 minutes under the above assumptions.


## Next steps

Possible next steps are

1) **Run on infiniband:** Validate network performance by running experiments on an infiniband enabled cluster or dig into AWS EFA for Monarch

2) **Scaling tests:** Scale up FUSE mount implementation to a lot of consumers on a single host (think 72 GPUs with multiple processes reading the same checkpoint)

3) **Fast serialization:** Reimplement serialization of directory in Rust or C to match performance of `find` or `mksquashfs`

4) **Fast append:** Implement fast append to deal with small local edits while Monarch workers are still running on allocated hosts. This should reduce the time it takes to restart a job with a new dependency or small change in a file to the time it takes to transfer those new or changed files. With persistent remote workers and fast transfer that means we could restart a job in seconds under small changes.

5) **Other filesystems:** Further explore alternative filesystems to back this mount (squashfs, ext, etc.)

6) **Integration tests:** Use remotemount in the context of a more advanced real-life project to search for further limitations or bugs.

7) **Compression:** Explore fast on-the-fly compression to reduce size of serialized buffer and increase transfer speeds.

8) **Faster mount point:** Reimplement our FUSE server by using FUSE passthrough and leverage its ability to remove copies for read and mmap operations.


## Appendix


### Issues we found along the way

1) rpath issues for nightly (from source build failed) - Reproducible issue filed

Filed an issue that we still need to resolve.

2) had to update slurm script (rpath and cpu-bind issue) - Likely resolved along with 1)

3) libfuse2 vs. libfuse3: fusepy uses libfuse2 - A rust or C reimplementation can be built on libfuse3

libfuse2 isn't available on fair-sc. This caused a silent ABI issue, which caused a weird bug.

The first version of this FUSE mount was developed on a devgpu with libfuse2 available.

3) aws efa ("fake infiniband") - Monarch RDMA support for AWS is a known issue

all pieces are there for Monarch RDMA to stop with QP failure, which made it hard to debug.

4) silent termination of large messages - Reproducible issue filed

The worker would simply get killed silently. Sending multiple chunks over multiple messages instead of one big chunk resolved this.

5) Running FUSE in the same process group as BashActor caused the whole thing to freeze. - Minimal reproducer is still missing and an issue needs to be filed. Using two separate process groups mitigates this and it might be a limitation of FUSE itself.

6) The entire time I had to double check it wasn't my weird application.

This seems like a normal issue when wrapping something around a piece of code. Using Monarch's this_host helps with this, but for issue 5) above it was necessary to write a standalone variant to keep sanity.

