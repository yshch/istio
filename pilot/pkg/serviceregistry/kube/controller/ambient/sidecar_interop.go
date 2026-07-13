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

	"istio.io/istio/pilot/pkg/features"
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
	ClusterID          cluster.ID
	WaypointServiceKey string
	WaypointInstance   []model.WorkloadInfo
	UseWaypoint        bool
}

func (s serviceEDS) ResourceName() string {
	if s.ClusterID != "" {
		return string(s.ClusterID) + "/" + s.ServiceKey
	}
	return s.ServiceKey
}

func (s serviceEDS) Equals(other serviceEDS) bool {
	if s.ServiceKey != other.ServiceKey || s.ClusterID != other.ClusterID ||
		s.WaypointServiceKey != other.WaypointServiceKey {
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
		s.ClusterID == other.ClusterID &&
		s.WaypointServiceKey == other.WaypointServiceKey &&
		s.UseWaypoint == other.UseWaypoint
}

// registerEdsShimPushes requests a full push when waypoint ownership changes,
// since sidecar interop changes EDS, RDS, CDS, and LDS in that case. Changes to
// replicas of the same waypoint remain incremental EDS updates.
func registerEdsShimPushes(serviceEds krt.Collection[serviceEDS], xdsUpdater model.XDSUpdater) {
	serviceEds.RegisterBatch(func(events []krt.Event[serviceEDS]) {
		if req := serviceEDSPushRequest(events); req != nil {
			xdsUpdater.ConfigUpdate(req)
		}
	}, false)
}

// registerMergedShimPushes complements the cluster-aware sidecar shim. The
// clustered shim owns binding additions, removals, identity changes, and
// waypoint replica/workload changes. The merged shim retains only ingress
// opt-in changes, which are not represented by the clustered service binding.
// This must only be used when the clustered shim is registered as well.
func registerMergedShimPushes(serviceEds krt.Collection[serviceEDS], xdsUpdater model.XDSUpdater) {
	serviceEds.RegisterBatch(func(events []krt.Event[serviceEDS]) {
		if req := mergedShimPushRequest(events); req != nil {
			xdsUpdater.ConfigUpdate(req)
		}
	}, false)
}

func mergedShimPushRequest(events []krt.Event[serviceEDS]) *model.PushRequest {
	remaining := slices.Filter(events, func(event krt.Event[serviceEDS]) bool {
		if event.Old == nil || event.New == nil {
			return false
		}
		return event.Old.ServiceKey == event.New.ServiceKey &&
			event.Old.UseWaypoint != event.New.UseWaypoint
	})
	return serviceEDSPushRequest(remaining)
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
	onlyIngressSelectionChanges bool,
	Workloads krt.Collection[model.WorkloadInfo],
	Namespaces krt.Collection[model.NamespaceInfo],
	WorkloadsByServiceKey krt.Index[string, model.WorkloadInfo],
	Services krt.Collection[model.ServiceInfo],
	ServicesByAddress krt.Index[networkAddress, model.ServiceInfo],
	opts krt.OptionsBuilder,
) {
	// Helps us avoid race conditions in tests.
	// Also, this flag shouldn't change once initialized, so this
	// has slightly better cache locality.
	multiNetworkEnabled := features.EnableAmbientMultiNetwork
	ServiceEds := krt.NewCollection(
		Services,
		func(ctx krt.HandlerContext, svc model.ServiceInfo) *serviceEDS {
			useWaypoint := ingressUseWaypoint(svc, krt.FetchOne(ctx, Namespaces, krt.FilterKey(svc.Service.Namespace)))
			if !includeAllServiceWaypoints && !useWaypoint && (!multiNetworkEnabled || svc.Scope != model.Global) {
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
	if onlyIngressSelectionChanges {
		registerMergedShimPushes(ServiceEds, xdsUpdater)
	} else {
		registerEdsShimPushes(ServiceEds, xdsUpdater)
	}
}

// clusteredServiceInfo gives each unmerged service representation a unique
// collection key. Replicated services otherwise share namespace/hostname and
// would be collapsed before their cluster-specific waypoint dependencies can
// be tracked.
type clusteredServiceInfo struct {
	ClusterID cluster.ID
	Service   model.ServiceInfo
}

// clusterServiceKey identifies one unmerged service representation.
// String implements fmt.Stringer as required by KRT indexes with typed keys.
type clusterServiceKey struct {
	ClusterID  cluster.ID
	ServiceKey string
}

func (k clusterServiceKey) String() string {
	return string(k.ClusterID) + "/" + k.ServiceKey
}

// clusterNetworkAddress scopes a service address to its owning cluster.
type clusterNetworkAddress struct {
	ClusterID cluster.ID
	Address   networkAddress
}

func (k clusterNetworkAddress) String() string {
	return string(k.ClusterID) + "/" + k.Address.String()
}

func (s clusteredServiceInfo) ResourceName() string {
	return clusterServiceKey{ClusterID: s.ClusterID, ServiceKey: s.Service.ResourceName()}.String()
}

func (s clusteredServiceInfo) Equals(other clusteredServiceInfo) bool {
	return s.ClusterID == other.ClusterID && s.Service.Equals(other.Service)
}

// RegisterClusteredEdsShim triggers destination-service EDS updates from each
// cluster's unmerged service binding and local waypoint replicas. This is
// required for native ambient multicluster, where the ordinary merged service
// representation intentionally keeps the config-cluster waypoint binding.
func RegisterClusteredEdsShim(
	xdsUpdater model.XDSUpdater,
	Workloads krt.Collection[model.WorkloadInfo],
	Services krt.Collection[clusteredServiceInfo],
	ServicesByAddress krt.Index[clusterNetworkAddress, clusteredServiceInfo],
	opts krt.OptionsBuilder,
) {
	workloadsByClusterService := krt.NewIndex[string, model.WorkloadInfo](Workloads, "clusterService", func(w model.WorkloadInfo) []string {
		services := make([]string, 0, len(w.Workload.Services))
		for service := range w.Workload.Services {
			services = append(services, w.Workload.ClusterId+"/"+service)
		}
		return services
	})

	serviceEds := krt.NewCollection(Services, func(ctx krt.HandlerContext, svc clusteredServiceInfo) *serviceEDS {
		wp := svc.Service.Service.Waypoint
		if wp == nil {
			return nil
		}
		var waypointServiceKey string
		switch addr := wp.Destination.(type) {
		case *workloadapi.GatewayAddress_Hostname:
			waypointServiceKey = addr.Hostname.Namespace + "/" + addr.Hostname.Hostname
		case *workloadapi.GatewayAddress_Address:
			address := addr.Address
			key := clusterNetworkAddress{
				ClusterID: svc.ClusterID,
				Address: networkAddress{
					network: address.Network,
					ip:      mustByteIPToString(address.Address),
				},
			}
			waypointSvc := krt.FetchOne(ctx, Services, krt.FilterIndex(ServicesByAddress, key))
			if waypointSvc == nil {
				return nil
			}
			waypointServiceKey = waypointSvc.Service.ResourceName()
		}
		workloads := krt.Fetch(ctx, Workloads,
			krt.FilterIndex(workloadsByClusterService, string(svc.ClusterID)+"/"+waypointServiceKey))
		workloads = slices.SortBy(workloads, func(w model.WorkloadInfo) string {
			return w.Workload.Uid
		})
		return &serviceEDS{
			ServiceKey:         svc.Service.ResourceName(),
			ClusterID:          svc.ClusterID,
			WaypointServiceKey: waypointServiceKey,
			WaypointInstance:   workloads,
		}
	}, opts.WithName("ClusteredServiceEds")...)
	registerEdsShimPushes(serviceEds, xdsUpdater)
}

func (a *index) ServicesWithWaypoint(key string, clusterID cluster.ID) []model.ServiceWaypointInfo {
	res := []model.ServiceWaypointInfo{}
	var svcs []model.ServiceInfo
	servicesByAddress := a.services.ByAddress.Lookup
	if clusterID != "" && a.services.Clustered != nil {
		// Native ambient multicluster merges same-key services and
		// intentionally bases the result on the config-cluster ServiceInfo.
		// Use the precomputed unmerged indexes so a remote sidecar follows its
		// own service binding without enumerating every service in its cluster.
		var clustered []clusteredServiceInfo
		if key == "" {
			clustered = a.services.ClusteredByCluster.Lookup(clusterID)
		} else {
			clustered = a.services.ClusteredByKey.Lookup(clusterServiceKey{
				ClusterID:  clusterID,
				ServiceKey: key,
			})
		}
		svcs = slices.Map(clustered, func(svc clusteredServiceInfo) model.ServiceInfo {
			return svc.Service
		})
		servicesByAddress = func(address networkAddress) []model.ServiceInfo {
			waypoints := a.services.ClusteredByAddress.Lookup(clusterNetworkAddress{
				ClusterID: clusterID,
				Address:   address,
			})
			return slices.Map(waypoints, func(svc clusteredServiceInfo) model.ServiceInfo {
				return svc.Service
			})
		}
	} else if key == "" {
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
			waypoints := servicesByAddress(wpNetAddr)
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
