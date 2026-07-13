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

// nolint: gocritic
package ambient

import (
	"net/netip"
	"strings"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/ptr"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/util/sets"
	"istio.io/istio/pkg/workloadapi"
)

// serviceEDS represents a service and a list of all instances of waypoints.
// For example, we may have ServiceKey=httpbin.org and WaypointInstance=[list of *waypoint workloads* attached].
// This is to map to the eventual EDS structure.
type serviceEDS struct {
	ServiceKey         string
	WaypointServiceKey string
	WaypointInstance   []model.WorkloadInfo
	UseWaypoint        bool
}

func (s serviceEDS) ResourceName() string {
	return s.ServiceKey
}

func (s serviceEDS) Equals(other serviceEDS) bool {
	if s.ServiceKey != other.ServiceKey || s.WaypointServiceKey != other.WaypointServiceKey {
		return false
	}
	if s.UseWaypoint != other.UseWaypoint {
		return false
	}
	if len(s.WaypointInstance) != len(other.WaypointInstance) {
		return false
	}
	// The builder sorts both slices by workload UID.
	for i := range s.WaypointInstance {
		if !s.WaypointInstance[i].Equals(other.WaypointInstance[i]) {
			return false
		}
	}
	return true
}

func (s serviceEDS) sameWaypointBinding(other serviceEDS) bool {
	return s.ServiceKey == other.ServiceKey &&
		s.WaypointServiceKey == other.WaypointServiceKey &&
		s.UseWaypoint == other.UseWaypoint
}

// registerEdsShimPushes requests a full push when waypoint ownership changes,
// since sidecar interop changes EDS, RDS, CDS, and LDS in that case. Changes to
// replicas of the same waypoint remain incremental EDS updates.
func registerEdsShimPushes(serviceEds krt.Collection[serviceEDS], xdsUpdater model.XDSUpdater) {
	serviceEds.RegisterBatch(func(events []krt.Event[serviceEDS], _ bool) {
		if req := serviceEDSPushRequest(events); req != nil {
			xdsUpdater.ConfigUpdate(req)
		}
	}, false)
}

func serviceEDSPushRequest(events []krt.Event[serviceEDS]) *model.PushRequest {
	configs := sets.New[model.ConfigKey]()
	full := false
	for _, event := range events {
		if event.Old == nil || event.New == nil || !event.Old.sameWaypointBinding(*event.New) {
			full = true
		}
		for _, svc := range event.Items() {
			ns, hostname, _ := strings.Cut(svc.ServiceKey, "/")
			configs.Insert(model.ConfigKey{Kind: kind.ServiceEntry, Name: hostname, Namespace: ns})
		}
	}
	if configs.IsEmpty() {
		return nil
	}
	return &model.PushRequest{
		Full:           full,
		ConfigsUpdated: configs,
		Reason:         model.NewReasonStats(model.AmbientUpdate),
	}
}

// RegisterEdsShim handles triggering xDS events when Envoy EDS needs to change.
// Most of ambient index works to build `workloadapi` types - Workload, Service, etc.
// Envoy uses a different API, with different relationships between types.
// To ensure Envoy are updated properly on changes, we compute this information.
// Currently, this is only used to trigger events.
// Ideally, the information we are using in Envoy and the event trigger are using the same data directly.
func RegisterEdsShim(
	xdsUpdater model.XDSUpdater,
	includeAllServiceWaypoints bool,
	Workloads krt.Collection[model.WorkloadInfo],
	Namespaces krt.Collection[model.NamespaceInfo],
	WorkloadsByServiceKey krt.Index[string, model.WorkloadInfo],
	Services krt.Collection[model.ServiceInfo],
	ServicesByAddress krt.Index[networkAddress, model.ServiceInfo],
	opts krt.OptionsBuilder,
) {
	ServiceEds := krt.NewCollection(
		Services,
		func(ctx krt.HandlerContext, svc model.ServiceInfo) *serviceEDS {
			useWaypoint := ingressUseWaypoint(svc, krt.FetchOne(ctx, Namespaces, krt.FilterKey(svc.Service.Namespace)))
			if !includeAllServiceWaypoints && !useWaypoint {
				return nil
			}
			wp := svc.Service.Waypoint
			if wp == nil {
				return nil
			}
			var waypointServiceKey string
			switch addr := wp.Destination.(type) {
			// Easy case: waypoint is already a hostname. Just return it directly
			case *workloadapi.GatewayAddress_Hostname:
				hn := addr.Hostname
				waypointServiceKey = hn.Namespace + "/" + hn.Hostname
			// Hard case: waypoint is an IP address. Need to look it up.
			case *workloadapi.GatewayAddress_Address:
				wAddress := addr.Address
				serviceKey := networkAddress{
					network: wAddress.Network,
					ip:      mustByteIPToString(wAddress.Address),
				}
				waypointSvc := krt.FetchOne(ctx, Services, krt.FilterIndex(ServicesByAddress, serviceKey))
				if waypointSvc == nil {
					return nil
				}
				waypointServiceKey = waypointSvc.ResourceName()
			}
			workloads := krt.Fetch(ctx, Workloads, krt.FilterIndex(WorkloadsByServiceKey, waypointServiceKey))
			// serviceEDS.Equals compares workloads positionally, so normalize the
			// non-deterministic index result before constructing the value.
			workloads = slices.SortBy(workloads, func(i model.WorkloadInfo) string {
				return i.Workload.Uid
			})
			return &serviceEDS{
				ServiceKey:         svc.ResourceName(),
				WaypointServiceKey: waypointServiceKey,
				UseWaypoint:        useWaypoint,
				WaypointInstance:   workloads,
			}
		},
		opts.WithName("ServiceEds")...)
	registerEdsShimPushes(ServiceEds, xdsUpdater)
}

func (a *index) ServicesWithWaypoint(key string, _ cluster.ID) []model.ServiceWaypointInfo {
	res := []model.ServiceWaypointInfo{}
	var svcs []model.ServiceInfo
	if key == "" {
		svcs = a.services.List()
	} else {
		svcs = ptr.ToList(a.services.GetKey(key))
	}
	for _, s := range svcs {
		wp := s.Service.GetWaypoint()
		useWaypoint := ingressUseWaypoint(s, a.namespaces.GetKey(s.Service.Namespace))
		wi := model.ServiceWaypointInfo{
			Service:            s.Service,
			IngressUseWaypoint: useWaypoint,
		}
		if wp == nil {
			continue
		}
		switch addr := wp.GetDestination().(type) {
		// Easy case: waypoint is already a hostname. Just return it directly
		case *workloadapi.GatewayAddress_Hostname:
			wi.WaypointHostname = addr.Hostname.Hostname
		// Hard case: waypoint is an IP address. Need to look it up.
		case *workloadapi.GatewayAddress_Address:
			wpAddr, _ := netip.AddrFromSlice(addr.Address.Address)
			wpNetAddr := networkAddress{
				network: addr.Address.Network,
				ip:      wpAddr.String(),
			}
			waypoints := a.services.ByAddress.Lookup(wpNetAddr)
			if len(waypoints) == 0 {
				// No waypoint found.
				continue
			}
			wi.WaypointHostname = waypoints[0].Service.Hostname
		}
		res = append(res, wi)
	}
	return res
}

func ingressUseWaypoint(s model.ServiceInfo, ns *model.NamespaceInfo) bool {
	if s.Waypoint.IngressLabelPresent {
		return s.Waypoint.IngressUseWaypoint
	}
	if ns != nil {
		return ns.IngressUseWaypoint
	}
	return false
}
