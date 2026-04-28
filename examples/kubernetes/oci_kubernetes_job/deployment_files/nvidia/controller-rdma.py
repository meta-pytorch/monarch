# %%
# Imports
# -------
# We import Monarch's Kubernetes job support and SPMDActor.

import argparse
import asyncio
import textwrap

from kubernetes.client import (
    V1Capabilities,
    V1Container,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1HostPathVolumeSource,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)

from monarch.config import configure
from monarch.job.kubernetes import KubernetesJob
from monarch.spmd import SPMDActor
from monarch.tools.network import AddrType

configure(enable_log_forwarding=True, message_delivery_timeout="2m")

_WORKER_BOOTSTRAP_SCRIPT: str = textwrap.dedent("""\
    import os
    import socket
    from monarch.actor import run_worker_loop_forever
    port = os.environ.get("MONARCH_PORT", "26600")
    hostname = socket.getfqdn()
    address = f"tcp://{hostname}:{port}"
    run_worker_loop_forever(address=address, ca="trust_all_connections")
""")

IMAGE = "ghcr.io/dochakov-oci/monarch-oci:monarch0.4.0-cuda12.8-rdma-01"

# Path to train.py on worker pods
TRAIN_SCRIPT = "/tmp/train.py"

# Training script content — written to worker pods at startup when provisioning
_TRAIN_SCRIPT_CONTENT = textwrap.dedent("""\
import warnings
warnings.filterwarnings("ignore", message="Using the native apex kernel for RoPE.")
                                        
import argparse
from glob import glob
from sklearn.metrics import precision_score, recall_score, accuracy_score, f1_score
import numpy as np
import os
import timm
from PIL import Image

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import torchvision.transforms as transforms
from torchvision.datasets import ImageFolder
import torch.multiprocessing as mp
from torch.utils.data.distributed import DistributedSampler
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.distributed import init_process_group, destroy_process_group

                                        
IMAGE_SIZE_PX = 1024

                                        
def ddp_setup():
    init_process_group(backend="nccl")


class Trainer:
    def __init__(
        self,
        model: torch.nn.Module,
        train_data: DataLoader,
        optimizer: torch.optim.Optimizer,
        save_every: int,
        snapshot_path: str
    ) -> None:
        self.local_rank = int(os.environ["LOCAL_RANK"])
        self.global_rank = int(os.environ["RANK"])
        self.model = model.to(self.local_rank)
        self.criterion = nn.CrossEntropyLoss()         # Customizable: Loss Function
        self.train_data = train_data
        self.optimizer = optimizer
        self.save_every = save_every
        self.epochs_run = 0

        if os.path.exists(snapshot_path):
            print(f"Loading snapshot from {snapshot_path}")
            self._load_snapshot(snapshot_path)

        self.model = DDP(self.model, device_ids=[self.local_rank])

    def _load_snapshot(self, snapshot_path):
        snapshot = torch.load(snapshot_path)

        self.model.load_state_dict(snapshot['MODEL_STATE'])
        self.epochs_run = snapshot['EPOCHS_RUN']

        print(f"Snapshot loaded. Resuming from epoch {self.epochs_run}")

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source)
        loss = self.criterion(output, targets)
        loss.backward()
        self.optimizer.step()

    def _run_epoch(self, epoch):
        print(f"[GPU{self.global_rank}] Epoch {epoch} | Batchsize: {self.train_data.batch_size} | Steps: {len(self.train_data)}")
        self.train_data.sampler.set_epoch(epoch)
        for source, targets in self.train_data:
            source = source.to(self.local_rank)
            targets = targets.to(self.local_rank)
            self._run_batch(source, targets)

    def _save_snapshot(self, epoch):
        snapshot = {}
        snapshot["MODEL_STATE"] = self.model.module.state_dict()
        snapshot["EPOCHS_RUN"] = epoch

        torch.save(snapshot, "snapshot.pt")

        print(f"Epoch {epoch} | Training checkpoint saved at snapshot.pt")

    def train(self, max_epochs: int):
        for epoch in range(self.epochs_run, max_epochs):
            self._run_epoch(epoch)
            if self.local_rank == 0 and epoch % self.save_every == 0:
                self._save_snapshot(epoch)


# Customizable: Dataset Helper Class
class PlayingCardDataset(Dataset):
    def __init__(self, data_dir, transform=None):
        self.data = ImageFolder(data_dir, transform=transform)
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        return self.data[idx]
    
    @property
    def classes(self):
        return self.data.classes


# Customizable: Model Class
class SimpleCardClassifer(nn.Module):
    def __init__(self, num_classes=53):
        super(SimpleCardClassifer, self).__init__()

        # Use timm's built-in num_classes to replace the classifier head cleanly
        self.base_model = timm.create_model('efficientnet_b0', pretrained=False, num_classes=num_classes)

    def forward(self, x):
        return self.base_model(x)


def load_train_objs():
    train_folder = os.path.expanduser('~/.cache/kagglehub/datasets/gpiosenka/cards-image-datasetclassification/versions/2/train')

    transform = transforms.Compose([
        transforms.Resize((IMAGE_SIZE_PX, IMAGE_SIZE_PX)),
        transforms.ToTensor(),
    ])
    
    train_set = PlayingCardDataset(train_folder, transform=transform)  # Customizable: Train Set
    model = SimpleCardClassifer(num_classes=53)                        # Customizable: Model
    optimizer = optim.Adam(model.parameters(), lr=0.001)               # Customizable: Optimizer
    
    return train_set, model, optimizer


def prepare_dataloader(dataset: Dataset, batch_size: int): #, world_size: int):
    return DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=True,
        shuffle=False,
        num_workers=2,
        # num_workers=torch.cuda.device_count()
        sampler=DistributedSampler(dataset),
        persistent_workers=True
    )

def main(save_every: int, total_epochs: int, batch_size: int, snapshot_path: str = "snapshot.pt"):
    ddp_setup()
    dataset, model, optimizer = load_train_objs()
    train_data = prepare_dataloader(dataset, batch_size) #, world_size)
    trainer = Trainer(model, train_data, optimizer, save_every, snapshot_path=snapshot_path)
    trainer.train(total_epochs)
    destroy_process_group()

    # Run validation on rank 0 only
    if int(os.environ["RANK"]) == 0:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        transform = transforms.Compose([
            transforms.Resize((IMAGE_SIZE_PX, IMAGE_SIZE_PX)),
            transforms.ToTensor()
        ])
        
        test_folder = os.path.expanduser('~/.cache/kagglehub/datasets/gpiosenka/cards-image-datasetclassification/versions/2/test')
        test_images = glob(test_folder + '/*/*')
        test_examples = np.random.choice(test_images, 100)
        
        all_true_labels = []
        all_pred_labels = []

        for example in test_examples:
            image = Image.open(example).convert("RGB")
            image_tensor = transform(image).unsqueeze(0)
            
            model.eval()
            with torch.no_grad():
                image_tensor = image_tensor.to(device)
                outputs = model(image_tensor)
                probabilities = torch.nn.functional.softmax(outputs, dim=1)
            
            class_names = dataset.classes
            
            true_label = os.path.basename(os.path.dirname(example))
            pred_label = class_names[torch.argmax(probabilities).item()]
            
            all_true_labels.append(true_label)
            all_pred_labels.append(pred_label)
        
        accuracy = accuracy_score(all_true_labels, all_pred_labels)
        precision = precision_score(all_true_labels, all_pred_labels, average='weighted', zero_division=0)
        recall = recall_score(all_true_labels, all_pred_labels, average='weighted', zero_division=0)
        f1 = f1_score(all_true_labels, all_pred_labels, average='weighted', zero_division=0)
        
        print(f"Accuracy:  {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall:    {recall:.4f}")
        print(f"F1 Score:  {f1:.4f}")


if __name__ == "__main__":
    save_every = 2
    total_epochs = 50
    batch_size = 32
    main(save_every, total_epochs, batch_size)
""")


def build_gpu_pod_spec(gpus_per_host: int) -> V1PodSpec:
    """Build a V1PodSpec for a MonarchMesh worker pod with GPU resources,
    shared memory, and RDMA/InfiniBand access for NCCL collectives across pods.

    Host networking and /dev/infiniband let NCCL reach the host RDMA NICs;
    IPC_LOCK plus privileged mode are required to pin memory for RDMA.

    The bootstrap command writes train.py to the worker filesystem
    before starting the Monarch worker loop, so the SPMDActor can
    find and execute it.
    """
    # Write train.py then start the worker loop
    bootstrap = (
        "import pathlib\n"
        f"pathlib.Path({TRAIN_SCRIPT!r}).write_text({_TRAIN_SCRIPT_CONTENT!r})\n"
        + _WORKER_BOOTSTRAP_SCRIPT
    )
    gpu_resources = {
        "nvidia.com/gpu": str(gpus_per_host),
        # Request one VF per physical RDMA port on the node (16 CX-6 data-plane
        # ports per a100-sriov-policy.yaml), not one per GPU. Requires one
        # Monarch pod per node since each pod consumes the node's full VF pool.
        "nvidia.com/sriov-rdma-vf": "16",
    }
    # hostNetwork is deliberately NOT set on worker pods. The Monarch K8s
    # operator discovers workers via pod DNS, and hostNetwork breaks that
    # (socket.getfqdn() resolves to the node FQDN, not the pod DNS name).
    # RDMA is provided via SR-IOV: requesting nvidia.com/sriov-rdma-vf
    # triggers the monarch-sriov-vf-injector webhook, which adds the
    # Multus annotation k8s.v1.cni.cncf.io/networks=sriov-rdma-vf so a
    # Mellanox VF netdev is plumbed into the pod netns for NCCL.
    return V1PodSpec(
        containers=[
            V1Container(
                name="worker",
                image=IMAGE,
                command=["python", "-u", "-c", bootstrap],
                env=[
                    V1EnvVar(name="MONARCH_PORT", value="26600"),
                    V1EnvVar(name="NCCL_IB_DISABLE", value="0"),
                    V1EnvVar(name="NCCL_IB_HCA", value="mlx5"),
                    V1EnvVar(name="NCCL_IB_GID_INDEX", value="3"),
                    V1EnvVar(name="NCCL_IB_TC", value="41"),
                    V1EnvVar(name="NCCL_IB_SL", value="0"),
                    V1EnvVar(name="NCCL_IB_QPS_PER_CONNECTION", value="4"),
                    V1EnvVar(name="NCCL_NET_GDR_LEVEL", value="PHB"),
                    V1EnvVar(name="NCCL_DEBUG", value="INFO"),
                ],
                security_context=V1SecurityContext(
                    privileged=True,
                    capabilities=V1Capabilities(add=["IPC_LOCK"]),
                ),
                resources=V1ResourceRequirements(
                    limits=gpu_resources,
                    requests=gpu_resources,
                ),
                volume_mounts=[
                    V1VolumeMount(name="dshm", mount_path="/dev/shm"),
                    V1VolumeMount(name="infiniband", mount_path="/dev/infiniband"),
                ],
            )
        ],
        volumes=[
            V1Volume(
                name="dshm",
                empty_dir=V1EmptyDirVolumeSource(medium="Memory", size_limit="16Gi"),
            ),
            V1Volume(
                name="infiniband",
                host_path=V1HostPathVolumeSource(path="/dev/infiniband"),
            ),
        ],
    )


# %%
# Main Function
# -------------
# The main function connects to Kubernetes pods and runs DDP training
# using ``SPMDActor`` to execute the training script.


async def main(
    num_hosts: int = 2,
    gpus_per_host: int = 8,
    mesh_name: str = "monarchmesh",
    provision: bool = False,
) -> None:
    """Run DDP training on Kubernetes.

    Args:
        num_hosts: Number of worker pods (must match MonarchMesh replicas)
        gpus_per_host: GPUs per pod (must match nvidia.com/gpu in MonarchMesh)
        mesh_name: Name of the MonarchMesh resource
        provision: If True, create MonarchMesh CRDs from Python
    """
    print("=" * 60)
    print("Kubernetes DDP Example")
    print(f"Configuration: {num_hosts} hosts, {gpus_per_host} GPUs/host")
    print("=" * 60)

    # %%
    # Connect to Kubernetes
    # ~~~~~~~~~~~~~~~~~~~~~
    # Create a ``KubernetesJob`` in the ``monarch-tests`` namespace.
    # With ``--provision``, the job creates MonarchMesh CRDs via the K8s API
    # using ``pod_spec`` for full control over the pod template (needed for
    # the shared memory volume that NCCL requires). Without ``--provision``,
    # it attaches to pre-provisioned pods.

    k8s_job = KubernetesJob(namespace="monarch-tests")
    if provision:
        k8s_job.add_mesh(
            mesh_name,
            num_replicas=num_hosts,
            pod_spec=build_gpu_pod_spec(gpus_per_host),
        )
    else:
        k8s_job.add_mesh(mesh_name, num_replicas=num_hosts)

    # %%
    # Create Process Mesh
    # ~~~~~~~~~~~~~~~~~~~
    # Get the job state and spawn processes on the workers. Each host gets
    # ``gpus_per_host`` processes, one per GPU.

    job_state = k8s_job.state()
    host_mesh = getattr(job_state, mesh_name)
    proc_mesh = host_mesh.spawn_procs({"gpus": gpus_per_host})

    # Stream logs from all processes to the client
    await proc_mesh.logging_option(stream_to_client=True)

    # %%
    # Run DDP Training with SPMDActor
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Spawn ``SPMDActor`` on the process mesh. The actor configures torch elastic
    # environment variables (RANK, LOCAL_RANK, WORLD_SIZE, MASTER_ADDR, MASTER_PORT)
    # and executes the training script.

    spmd_actors = proc_mesh.spawn("_SPMDActor", SPMDActor)

    # Get master address/port from first actor (all coordinates = 0)
    # We use IPv4 addresses since short hostnames may not resolve across pods.
    first_values = dict.fromkeys(proc_mesh._labels, 0)
    master_addr, master_port = await spmd_actors.slice(
        **first_values
    ).get_host_port.call_one(AddrType.IPv4)

    # Execute training script across the mesh
    await spmd_actors.main.call(master_addr, master_port, [TRAIN_SCRIPT])

    print("=" * 60)
    print("DDP example completed successfully!")
    print("=" * 60)

    # Clean up
    proc_mesh.stop().get()

    if provision:
        k8s_job.kill()


# %%
# Running the Example
# -------------------
#
# Command-line Arguments
# ~~~~~~~~~~~~~~~~~~~~~~
# - ``--provision``: Create MonarchMesh CRDs from Python (no worker YAML needed)
# - ``--num_hosts``: Number of worker pods
# - ``--gpus_per_host``: GPUs per pod
# - ``--mesh_name``: Name of the MonarchMesh resource

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run DDP training on Kubernetes")
    parser.add_argument(
        "--num_hosts",
        type=int,
        default=2,
        help="Number of worker pods",
    )
    parser.add_argument(
        "--gpus_per_host",
        type=int,
        default=4,
        help="GPUs per pod",
    )
    parser.add_argument(
        "--mesh_name",
        type=str,
        default="monarchmesh",
        help="Name of the MonarchMesh resource",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision MonarchMesh CRDs from Python (no YAML manifests needed)",
    )
    args = parser.parse_args()
    asyncio.run(
        main(args.num_hosts, args.gpus_per_host, args.mesh_name, args.provision)
    )