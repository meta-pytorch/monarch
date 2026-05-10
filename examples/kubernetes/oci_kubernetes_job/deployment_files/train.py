# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import warnings

warnings.filterwarnings("ignore", message="Using the native apex kernel for RoPE.")

import argparse
import os
from glob import glob

import numpy as np
import timm
import torch
import torch.multiprocessing as mp
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torchvision.transforms as transforms
from PIL import Image
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from torch.distributed import destroy_process_group, init_process_group
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, Dataset
from torch.utils.data.distributed import DistributedSampler
from torchvision.datasets import ImageFolder


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
        snapshot_path: str,
    ) -> None:
        self.local_rank = int(os.environ["LOCAL_RANK"])
        self.global_rank = int(os.environ["RANK"])
        self.model = model.to(self.local_rank)
        self.criterion = nn.CrossEntropyLoss()  # Customizable: Loss Function
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

        self.model.load_state_dict(snapshot["MODEL_STATE"])
        self.epochs_run = snapshot["EPOCHS_RUN"]

        print(f"Snapshot loaded. Resuming from epoch {self.epochs_run}")

    def _run_batch(self, source, targets):
        self.optimizer.zero_grad()
        output = self.model(source)
        loss = self.criterion(output, targets)
        loss.backward()
        self.optimizer.step()

    def _run_epoch(self, epoch):
        print(
            f"[GPU{self.global_rank}] Epoch {epoch} | Batchsize: {self.train_data.batch_size} | Steps: {len(self.train_data)}"
        )
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
        self.base_model = timm.create_model(
            "efficientnet_b0", pretrained=False, num_classes=num_classes
        )

    def forward(self, x):
        return self.base_model(x)


def load_train_objs():
    train_folder = os.path.expanduser(
        "~/.cache/kagglehub/datasets/gpiosenka/cards-image-datasetclassification/versions/2/train"
    )

    transform = transforms.Compose(
        [
            transforms.Resize((IMAGE_SIZE_PX, IMAGE_SIZE_PX)),
            transforms.ToTensor(),
        ]
    )

    train_set = PlayingCardDataset(
        train_folder, transform=transform
    )  # Customizable: Train Set
    model = SimpleCardClassifer(num_classes=53)  # Customizable: Model
    optimizer = optim.Adam(model.parameters(), lr=0.001)  # Customizable: Optimizer

    return train_set, model, optimizer


def prepare_dataloader(dataset: Dataset, batch_size: int):  # , world_size: int):
    return DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=True,
        shuffle=False,
        num_workers=2,
        # num_workers=torch.cuda.device_count(),
        sampler=DistributedSampler(dataset),
        persistent_workers=True,
    )


def main(
    save_every: int,
    total_epochs: int,
    batch_size: int,
    snapshot_path: str = "snapshot.pt",
):
    ddp_setup()
    dataset, model, optimizer = load_train_objs()
    train_data = prepare_dataloader(dataset, batch_size)  # , world_size)
    trainer = Trainer(
        model, train_data, optimizer, save_every, snapshot_path=snapshot_path
    )
    trainer.train(total_epochs)
    destroy_process_group()

    # Run validation on rank 0 only
    if int(os.environ["RANK"]) == 0:
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        transform = transforms.Compose(
            [transforms.Resize((IMAGE_SIZE_PX, IMAGE_SIZE_PX)), transforms.ToTensor()]
        )

        test_folder = os.path.expanduser(
            "~/.cache/kagglehub/datasets/gpiosenka/cards-image-datasetclassification/versions/2/test"
        )
        test_images = glob(test_folder + "/*/*")
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
        precision = precision_score(
            all_true_labels, all_pred_labels, average="weighted", zero_division=0
        )
        recall = recall_score(
            all_true_labels, all_pred_labels, average="weighted", zero_division=0
        )
        f1 = f1_score(
            all_true_labels, all_pred_labels, average="weighted", zero_division=0
        )

        print(f"Accuracy:  {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall:    {recall:.4f}")
        print(f"F1 Score:  {f1:.4f}")


if __name__ == "__main__":
    save_every = 2
    total_epochs = 50
    batch_size = 32
    main(save_every, total_epochs, batch_size)
