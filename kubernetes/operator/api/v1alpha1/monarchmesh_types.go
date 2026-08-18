/*
BSD 3-Clause License

Copyright (c) Meta Platforms, Inc. and affiliates.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MonarchMeshSpec defines the desired state of MonarchMesh
type MonarchMeshSpec struct {
	// Replicas is the number of Monarch worker pods to run.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Required
	Replicas int32 `json:"replicas"`

	// Port is the port that Monarch workers listen on for mesh communication.
	// +kubebuilder:default=26600
	// +optional
	Port int32 `json:"port,omitempty"`

	// PodTemplate defines the pod specification for Monarch workers.
	// Labels and annotations are inherited from the MonarchMesh metadata.
	PodTemplate corev1.PodSpec `json:"podTemplate"`
}

// MonarchMeshStatus defines the observed state of MonarchMesh.
type MonarchMeshStatus struct {
	// Replicas is the total number of pods targeted by this MonarchMesh.
	// +optional
	Replicas int32 `json:"replicas"`

	// ReadyReplicas is the number of pods that are ready and running.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// Conditions represent the current state of the MonarchMesh resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:path=monarchmeshes,scope=Namespaced

// MonarchMesh is the Schema for the monarchmeshes API
type MonarchMesh struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MonarchMesh
	// +required
	Spec MonarchMeshSpec `json:"spec"`

	// status defines the observed state of MonarchMesh
	// +optional
	Status MonarchMeshStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MonarchMeshList contains a list of MonarchMesh
type MonarchMeshList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MonarchMesh `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MonarchMesh{}, &MonarchMeshList{})
}
