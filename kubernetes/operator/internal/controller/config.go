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

package controller

// Config holds configuration for the MonarchMesh controller.
// These values can be overridden via controller flags in a future iteration.
type Config struct {
	// MeshLabelKey is the FQDN label key used to identify MonarchMesh-owned resources.
	// The value of this label will be set to the MonarchMesh name.
	// Uses FQDN convention to avoid collisions per:
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
	MeshLabelKey string

	// AppLabelKey is the standard Kubernetes label key for application name.
	AppLabelKey string

	// AppLabelValue is the value for the app label on pods.
	// This is used for pod selection and identification.
	AppLabelValue string

	// DefaultPort is the default port for Monarch mesh communication
	// when not specified in the MonarchMesh spec.
	DefaultPort int32

	// ServiceSuffix is appended to the MonarchMesh name to form the headless service name.
	ServiceSuffix string

	// PortName is the name used for the service port.
	PortName string
}

// DefaultConfig returns the default controller configuration.
// These defaults are suitable for most Monarch deployments.
func DefaultConfig() Config {
	return Config{
		MeshLabelKey:  "monarch.pytorch.org/mesh-name",
		AppLabelKey:   "app.kubernetes.io/name",
		AppLabelValue: "monarch-worker",
		DefaultPort:   26600,
		ServiceSuffix: "-svc",
		PortName:      "monarch",
	}
}
