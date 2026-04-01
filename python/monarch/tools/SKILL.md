Quick start:

  # Create a job config file (e.g. myjob.py)
  from monarch._src.job.process import ProcessJob
  job = ProcessJob({"hosts": 2})
  job.remote_mount("/path/to/src", mntpoint="/path/to/mnt")
  job.gather_mount("/worker/output", "/local/gathered")

  # Apply (provision) the job — argument is module.attribute import path
  monarch apply myjob.job

  # Run a command on rank 0 (streams output)
  monarch exec python train.py

  # Run on all ranks (output goes to per-rank log files)
  monarch exec --all python train.py

  # Spawn 4 GPU processes per host and run on all of them
  monarch exec --all --per-host gpu=4 python train.py

  # Run a bash script
  monarch exec --script run.sh

  # Kill the job when done
  monarch kill

Commands:

  apply   Provision workers from a job config Python file
  exec    Run a command on workers
  kill    Kill the active job
  context Manage named job contexts

Job reuse:
  Worker allocation is slow (minutes). Use "monarch exec" to reuse workers
  across runs. Only run "monarch apply" when you need a new allocation.
  Use "--kill" only when you are done with the workers entirely.

exec options:

  Targeting (mutually exclusive; default is --one):
    (default)         Run on rank 0 of the first mesh, stream output
    --all             Run on all meshes and all ranks; output → files
    --mesh NAME       Run on all ranks of the named mesh; output → files
    --point DIM=N,..  Run on a specific coordinate (e.g. host=4,gpu=3), stream output

  --per-host DIM=N    Spawn N processes per host along dimension DIM before
                      executing (e.g. --per-host gpu=4). The environment
                      variables MONARCH_RANK_<DIM> and MONARCH_SIZE_<DIM>
                      are set for each dimension of the actor's rank.

  -e KEY=VALUE        Extra environment variable (repeatable)
  --workdir DIR       Working directory on workers
  --script FILE       Read bash script from FILE (use '-' for stdin)
  --kill              Kill the job after the command finishes
