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

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	monarchv1alpha1 "github.com/meta-pytorch/monarch-kubernetes/api/v1alpha1"
)

// MonarchMeshReconciler reconciles a MonarchMesh object
type MonarchMeshReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config Config
}

// RBAC permissions for the controller.
// Each resource access is documented with its purpose:
//
// monarchmeshes (get;list;watch;create;update;patch;delete):
//   Required for full reconciliation of the custom resource. The controller reads the
//   spec to determine desired state and writes back to update observed state.
//
// monarchmeshes/status (get;update;patch):
//   Required to update the status subresource with replica counts (Replicas, ReadyReplicas)
//   and Conditions (Ready status).
//
// monarchmeshes/finalizers (update):
//   Reserved for future cleanup logic. Finalizers allow the controller to perform
//   cleanup before the resource is garbage collected.
//
// statefulsets (get;list;watch;create;update;patch;delete):
//   The controller creates and manages a StatefulSet for each MonarchMesh. StatefulSets
//   provide stable pod identities (mesh-0, mesh-1, etc.) and ordered/parallel pod management.
//
// services (get;list;watch;create;update;patch;delete):
//   The controller creates a headless Service for each MonarchMesh. The headless Service
//   enables DNS-based pod discovery (e.g., mesh-0.mesh-svc.namespace.svc.cluster.local).

// +kubebuilder:rbac:groups=monarch.pytorch.org,resources=monarchmeshes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monarch.pytorch.org,resources=monarchmeshes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=monarch.pytorch.org,resources=monarchmeshes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile ensures the cluster state matches the desired state specified in the MonarchMesh resource.
// It creates/updates a headless Service for DNS-based pod discovery and a StatefulSet for running
// Monarch worker pods with stable network identities.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *MonarchMeshReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the MonarchMesh object.
	// If not found, the object was deleted - cleanup is handled automatically via OwnerReferences
	// (Kubernetes garbage collection deletes owned StatefulSets and Services).
	var mesh monarchv1alpha1.MonarchMesh
	if err := r.Get(ctx, req.NamespacedName, &mesh); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Define identifiers and labels for owned resources.
	// Uses FQDN label convention to avoid collisions per:
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
	labels := map[string]string{
		r.Config.MeshLabelKey: mesh.Name,
		r.Config.AppLabelKey:  r.Config.AppLabelValue,
	}
	svcName := mesh.Name + r.Config.ServiceSuffix

	// Determine the port to use (default from config if not specified in CRD)
	port := mesh.Spec.Port
	if port == 0 {
		port = r.Config.DefaultPort
	}

	// 3. Ensure headless Service exists for DNS-based pod discovery.
	// The headless Service (ClusterIP: None) provides DNS entries like:
	// <pod-name>.<service-name>.<namespace>.svc.cluster.local
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: mesh.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.ClusterIP = "None"
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Name: r.Config.PortName, Port: port}}
		return ctrl.SetControllerReference(&mesh, svc, r.Scheme)
	})
	if err != nil {
		log.Error(err, "Failed to create or update Service")
		// Returning error automatically triggers requeue with exponential backoff
		return ctrl.Result{}, err
	}

	// 4. Ensure StatefulSet exists for running Monarch worker pods.
	// We use StatefulSet (not Deployment) because:
	// - Pods get stable, predictable names (mesh-0, mesh-1, etc.)
	// - Pods maintain identity across restarts
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: mesh.Name, Namespace: mesh.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = labels
		ss.Spec.Replicas = &mesh.Spec.Replicas
		ss.Spec.ServiceName = svcName
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		// Use Parallel pod management to launch all pods simultaneously rather than sequentially.
		// This can speed up large worker pod launches.
		// See: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#parallel-pod-management
		ss.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ss.Spec.Template.ObjectMeta.Labels = labels
		ss.Spec.Template.Spec = mesh.Spec.PodTemplate
		return ctrl.SetControllerReference(&mesh, ss, r.Scheme)
	})
	if err != nil {
		log.Error(err, "Failed to create or update StatefulSet")
		return ctrl.Result{}, err
	}

	// 5. Update MonarchMesh status with observed state from StatefulSet.
	// Status updates are triggered automatically when owned StatefulSet changes (via Owns()).
	mesh.Status.Replicas = ss.Status.Replicas
	mesh.Status.ReadyReplicas = ss.Status.ReadyReplicas

	condition := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Waiting"}
	if ss.Status.ReadyReplicas == mesh.Spec.Replicas {
		condition = metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllReady"}
	}
	meta.SetStatusCondition(&mesh.Status.Conditions, condition)

	if err := r.Status().Update(ctx, &mesh); err != nil {
		log.Error(err, "Failed to update MonarchMesh status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MonarchMeshReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&monarchv1alpha1.MonarchMesh{}).
		// Owns() watches StatefulSets that have an OwnerReference pointing to a MonarchMesh.
		// When a StatefulSet changes (e.g., pod becomes ready, status updates), controller-runtime
		// automatically looks up the OwnerReference and enqueues a reconcile for the parent MonarchMesh.
		// This triggers Reconcile(), which reads the latest StatefulSet status and copies it to
		// MonarchMesh.Status (Replicas, ReadyReplicas, Conditions).
		// See: https://book.kubebuilder.io/reference/watching-resources/owned
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
