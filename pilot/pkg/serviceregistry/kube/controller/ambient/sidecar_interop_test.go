// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ambient

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"istio.io/api/label"
	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/util/xdsfake"
	"istio.io/istio/pkg/config/constants"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/maps"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/test"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

func TestServiceEDSEquals(t *testing.T) {
	workload := func(uid string, labels map[string]string) model.WorkloadInfo {
		return model.WorkloadInfo{
			Workload: &workloadapi.Workload{Uid: uid},
			Labels:   labels,
		}
	}
	sorted := func(workloads ...model.WorkloadInfo) []model.WorkloadInfo {
		return slices.SortBy(workloads, func(w model.WorkloadInfo) string {
			return w.Workload.Uid
		})
	}

	base := serviceEDS{
		ServiceKey:         "ns/service.example.com",
		WaypointServiceKey: "ns/waypoint-a.example.com",
		WaypointInstance: sorted(
			workload("waypoint-b", map[string]string{"version": "v1"}),
			workload("waypoint-a", map[string]string{"version": "v1"}),
		),
		UseWaypoint: true,
	}
	reordered := serviceEDS{
		ServiceKey:         "ns/service.example.com",
		WaypointServiceKey: "ns/waypoint-a.example.com",
		WaypointInstance: sorted(
			workload("waypoint-a", map[string]string{"version": "v1"}),
			workload("waypoint-b", map[string]string{"version": "v1"}),
		),
		UseWaypoint: true,
	}
	if !base.Equals(reordered) {
		t.Fatal("workload fetch order should not affect equality after UID sorting")
	}

	metadataChanged := reordered
	metadataChanged.WaypointInstance = sorted(
		workload("waypoint-a", map[string]string{"version": "v2"}),
		workload("waypoint-b", map[string]string{"version": "v1"}),
	)
	if base.Equals(metadataChanged) {
		t.Fatal("metadata-only workload changes must affect equality")
	}

	waypointChanged := reordered
	waypointChanged.WaypointServiceKey = "ns/waypoint-b.example.com"
	if base.Equals(waypointChanged) {
		t.Fatal("waypoint identity changes must affect equality even with the same replicas")
	}

	clusterChanged := reordered
	clusterChanged.ClusterID = "cluster-2"
	if base.Equals(clusterChanged) {
		t.Fatal("cluster identity must distinguish per-cluster invalidation state")
	}
}

func TestServiceEDSPushRequest(t *testing.T) {
	base := serviceEDS{
		ServiceKey:         "ns/service.example.com",
		ClusterID:          "cluster-1",
		WaypointServiceKey: "ns/waypoint-a.example.com",
		UseWaypoint:        true,
	}
	withReplica := base
	withReplica.WaypointInstance = []model.WorkloadInfo{{Workload: &workloadapi.Workload{Uid: "waypoint-a"}}}
	otherWaypoint := base
	otherWaypoint.WaypointServiceKey = "ns/waypoint-b.example.com"
	otherCluster := base
	otherCluster.ClusterID = "cluster-2"

	cases := []struct {
		name  string
		event krt.Event[serviceEDS]
		full  bool
	}{
		{"ownership added", krt.Event[serviceEDS]{New: &base}, true},
		{"ownership removed", krt.Event[serviceEDS]{Old: &base}, true},
		{"selected waypoint changed", krt.Event[serviceEDS]{Old: &base, New: &otherWaypoint}, true},
		{"cluster binding changed", krt.Event[serviceEDS]{Old: &base, New: &otherCluster}, true},
		{"same waypoint replicas changed", krt.Event[serviceEDS]{Old: &base, New: &withReplica}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := serviceEDSPushRequest([]krt.Event[serviceEDS]{tt.event})
			if req == nil || req.Full != tt.full {
				t.Fatalf("request = %#v, want Full=%v", req, tt.full)
			}
			want := model.ConfigKey{Kind: kind.ServiceEntry, Name: "service.example.com", Namespace: "ns"}
			if !req.ConfigsUpdated.Contains(want) {
				t.Fatalf("configs = %v, want %v", req.ConfigsUpdated, want)
			}
		})
	}
}

func TestMergedShimPushRequest(t *testing.T) {
	base := serviceEDS{
		ServiceKey:         "ns/service.example.com",
		WaypointServiceKey: "ns/waypoint-a.example.com",
		UseWaypoint:        false,
	}
	ingressEnabled := base
	ingressEnabled.UseWaypoint = true
	otherWaypoint := ingressEnabled
	otherWaypoint.WaypointServiceKey = "ns/waypoint-b.example.com"
	withReplica := ingressEnabled
	withReplica.WaypointInstance = []model.WorkloadInfo{{Workload: &workloadapi.Workload{Uid: "waypoint-a"}}}
	metadataChanged := withReplica
	metadataChanged.WaypointInstance = []model.WorkloadInfo{{
		Workload: &workloadapi.Workload{Uid: "waypoint-a"},
		Labels:   map[string]string{"version": "v2"},
	}}

	if req := mergedShimPushRequest([]krt.Event[serviceEDS]{{Old: &base, New: &ingressEnabled}}); req == nil || !req.Full {
		t.Fatalf("ingress selection request = %#v, want full push", req)
	}
	for name, event := range map[string]krt.Event[serviceEDS]{
		"binding added":            {New: &ingressEnabled},
		"binding removed":          {Old: &ingressEnabled},
		"binding changed":          {Old: &ingressEnabled, New: &otherWaypoint},
		"replica added":            {Old: &ingressEnabled, New: &withReplica},
		"replica metadata changed": {Old: &withReplica, New: &metadataChanged},
		"replica removed":          {Old: &withReplica, New: &ingressEnabled},
	} {
		t.Run(name, func(t *testing.T) {
			if req := mergedShimPushRequest([]krt.Event[serviceEDS]{event}); req != nil {
				t.Fatalf("request = %#v, want nil; clustered shim owns this change", req)
			}
		})
	}
}

func TestWaypointInterop(t *testing.T) {
	for _, tt := range []struct {
		name          string
		enableFeature *bool
		serviceLabels map[string]string
	}{
		{
			name:          "IngressUseWaypoint",
			enableFeature: nil,
			serviceLabels: map[string]string{"istio.io/ingress-use-waypoint": "true"},
		},
		{
			name:          "AmbientMultiNetwork",
			enableFeature: &features.EnableAmbientMultiNetwork,
			serviceLabels: map[string]string{"istio.io/global": "true"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.enableFeature != nil {
				test.SetForTest(t, tt.enableFeature, true)
			}
			// Test that we can get updates for EDS when we have service bound waypoints.
			s := newAmbientTestServer(t, testC, testNW, "")
			// the two types return different keys.. rename to make it more clear
			addressUpdate := s.svcXdsName
			edsUpdate := s.hostnameForService
			assertServicesWithWaypoint := func(want ...string) {
				t.Helper()
				fetch := func() []string {
					got := s.ServicesWithWaypoint(s.svcXdsName("svc1"), s.clusterID)
					return slices.Map(got, func(e model.ServiceWaypointInfo) string {
						return e.Service.Hostname + "/" + e.WaypointHostname
					})
				}
				assert.EventuallyEqual(t, fetch, want)
			}

			s.addService(t, "svc1",
				maps.MergeCopy(map[string]string{label.IoIstioUseWaypoint.Name: "wp-svc"}, tt.serviceLabels),
				map[string]string{},
				[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
			s.assertEvent(t, addressUpdate("svc1"))
			assertServicesWithWaypoint()

			// Add waypoint...
			// We should get a service update for EDS to update
			// First we will test an IP-based waypoint...
			s.addWaypointSpecificAddress(t, "10.0.0.1", "", "wp-svc", constants.AllTraffic, true)
			s.addService(t, "wp-svc",
				map[string]string{},
				map[string]string{},
				[]int32{80}, map[string]string{"app": "waypoint"}, "10.0.0.1")
			ownershipEvents := []xdsfake.Event{
				{Type: "xds", ID: addressUpdate("wp-svc")},
				{Type: "xds", ID: addressUpdate("svc1")},
				{Type: "xds full", ID: edsUpdate("svc1")},
			}
			s.fx.MatchOrFail(t, ownershipEvents...)
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + s.hostnameForService("wp-svc"))

			// add a waypoint instance... we should get an EDS update
			s.addPods(t, "127.0.0.4", "wp-pod1", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
			s.assertEvent(t, s.podXdsName("wp-pod1"), edsUpdate("svc1"))
			s.addPods(t, "127.0.0.5", "wp-pod2", "wp-sa", map[string]string{"app": "waypoint"}, nil, true, corev1.PodRunning)
			s.assertEvent(t, s.podXdsName("wp-pod2"), edsUpdate("svc1"))
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + s.hostnameForService("wp-svc"))

			// A metadata-only change to an existing waypoint workload must also
			// invalidate the destination service's EDS without changing replica count.
			s.labelPod(t, "wp-pod1", testNS, map[string]string{"app": "waypoint", "version": "v2"})
			s.assertEvent(t, s.podXdsName("wp-pod1"), edsUpdate("svc1"))

			// now we are going to change to a different waypoint, this will be hostname based
			s.addWaypointSpecificAddress(t, "", "example.com", "wp-svc-host", constants.AllTraffic, true)
			s.addService(t, "svc1",
				maps.MergeCopy(map[string]string{label.IoIstioUseWaypoint.Name: "wp-svc-host"}, tt.serviceLabels),
				map[string]string{},
				[]int32{80}, map[string]string{"app": "a"}, "10.0.0.2")
			bindingEvents := []xdsfake.Event{
				{Type: "xds", ID: addressUpdate("svc1")},
				{Type: "xds full", ID: edsUpdate("svc1")},
			}
			s.fx.MatchOrFail(t, bindingEvents...)
			assertServicesWithWaypoint(s.hostnameForService("svc1") + "/" + "example.com")
		})
	}
}
