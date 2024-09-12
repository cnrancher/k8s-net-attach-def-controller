package controller

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/klog/v2"
	utilpointer "k8s.io/utils/pointer"

	"gopkg.in/k8snetworkplumbingwg/multus-cni.v3/pkg/types"
)

func objectChanged(previous, current interface{}) bool {
	prev := previous.(metav1.Object)
	cur := current.(metav1.Object)
	return prev.GetResourceVersion() != cur.GetResourceVersion()
}

func networkAnnotationsChanged(previous, current interface{}) bool {
	oldAnnotations := getNetworkAnnotations(previous)
	updatedAnnotations := getNetworkAnnotations(current)
	return oldAnnotations != updatedAnnotations
}

// FIXME
func networkStatusChanged(_, _ interface{}) bool {
	return true
}

func getNetworkAnnotations(obj interface{}) string {
	metaObject := obj.(metav1.Object)
	annotations, ok := metaObject.GetAnnotations()[nettypes.NetworkAttachmentAnnot]
	if !ok {
		return ""
	}
	return annotations
}

func isInNetworkSelectionElementsArray(name, namespace string, networks []*types.NetworkSelectionElement) bool {
	// https://github.com/k8snetworkplumbingwg/multus-cni/blob/v3.7.2/pkg/types/conf.go#L109
	var netName, netNamespace string
	units := strings.SplitN(name, "/", 2)
	switch len(units) {
	case 1:
		netName = units[0]
		netNamespace = namespace
	case 2:
		netNamespace = units[0]
		netName = units[1]
	default:
		err := errors.Errorf("invalid network status - '%s'", name)
		klog.Error(err)
		return false
	}
	for i := range networks {
		if netName == networks[i].Name && netNamespace == networks[i].Namespace {
			return true
		}
	}
	return false
}

// NOTE: two below functions are copied from the net-attach-def admission controller, to be replaced with better implementation
func parsePodNetworkSelections(podNetworks, defaultNamespace string) ([]*types.NetworkSelectionElement, error) {
	var networkSelections []*types.NetworkSelectionElement

	if len(podNetworks) == 0 {
		err := errors.New("empty string passed as network selection elements list")
		klog.Error(err)
		return nil, err
	}

	/* try to parse as JSON array */
	err := json.Unmarshal([]byte(podNetworks), &networkSelections)

	/* if failed, try to parse as comma separated */
	if err != nil {
		klog.V(4).Infof("'%s' is not in JSON format: %s... trying to parse as comma separated network selections list", podNetworks, err)
		for _, networkSelection := range strings.Split(podNetworks, ",") {
			networkSelection = strings.TrimSpace(networkSelection)
			networkSelectionElement, err := parsePodNetworkSelectionElement(networkSelection, defaultNamespace)
			if err != nil {
				err := errors.Wrap(err, "error parsing network selection element")
				klog.Error(err)
				return nil, err
			}
			networkSelections = append(networkSelections, networkSelectionElement)
		}
	}

	/* fill missing namespaces with default value */
	for _, networkSelection := range networkSelections {
		if networkSelection.Namespace == "" {
			networkSelection.Namespace = defaultNamespace
		}
	}

	return networkSelections, nil
}

func parsePodNetworkSelectionElement(selection, defaultNamespace string) (*types.NetworkSelectionElement, error) {
	var namespace, name, netInterface string
	var networkSelectionElement *types.NetworkSelectionElement

	units := strings.Split(selection, "/")
	switch len(units) {
	case 1:
		namespace = defaultNamespace
		name = units[0]
	case 2:
		namespace = units[0]
		name = units[1]
	default:
		err := errors.Errorf("invalid network selection element - more than one '/' rune in: '%s'", selection)
		klog.Error(err)
		return networkSelectionElement, err
	}

	units = strings.Split(name, "@")
	switch len(units) {
	case 1:
		name = units[0]
		netInterface = ""
	case 2:
		name = units[0]
		netInterface = units[1]
	default:
		err := errors.Errorf("invalid network selection element - more than one '@' rune in: '%s'", selection)
		klog.Error(err)
		return networkSelectionElement, err
	}

	validNameRegex, _ := regexp.Compile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	for _, unit := range []string{namespace, name, netInterface} {
		ok := validNameRegex.MatchString(unit)
		if !ok && len(unit) > 0 {
			err := errors.Errorf("at least one of the network selection units is invalid: error found at '%s'", unit)
			klog.Error(err)
			return networkSelectionElement, err
		}
	}

	networkSelectionElement = &types.NetworkSelectionElement{
		Namespace:        namespace,
		Name:             name,
		InterfaceRequest: netInterface,
	}

	return networkSelectionElement, nil
}

// epPortsToEpsPorts converts ports from an Endpoints resource to ports for an
// EndpointSlice resource.
func epPortsToEpsPorts(epPorts []corev1.EndpointPort) []discovery.EndpointPort {
	epsPorts := []discovery.EndpointPort{}
	for _, epPort := range epPorts {
		epp := epPort.DeepCopy()
		epsPorts = append(epsPorts, discovery.EndpointPort{
			Name:        &epp.Name,
			Port:        &epp.Port,
			Protocol:    &epp.Protocol,
			AppProtocol: epp.AppProtocol,
		})
	}
	return epsPorts
}

// addressToEndpoint converts an address from an Endpoints resource to an
// EndpointSlice endpoint.
func addressToEndpoint(address corev1.EndpointAddress) discovery.Endpoint {
	endpoint := discovery.Endpoint{
		Addresses: []string{address.IP},
		Conditions: discovery.EndpointConditions{
			Ready: utilpointer.BoolPtr(true),
		},
		TargetRef: address.TargetRef,
	}

	if address.NodeName != nil {
		endpoint.NodeName = address.NodeName
	}
	if address.Hostname != "" {
		endpoint.Hostname = &address.Hostname
	}

	return endpoint
}

func sortEpsEndpoints(eps []discovery.Endpoint) {
	sort.Slice(eps, func(i, j int) bool {
		return strings.Compare(eps[i].TargetRef.Name, eps[j].TargetRef.Name) > 0
	})
}

func sortEpsPorts(ports []discovery.EndpointPort) {
	sort.Slice(ports, func(i, j int) bool {
		return strings.Compare(*ports[i].Name, *ports[j].Name) > 0
	})
}

func filterEpsList(epsList *discovery.EndpointSliceList) []discovery.EndpointSlice {
	result := []discovery.EndpointSlice{}

	for _, eps := range epsList.Items {
		if eps.AddressType != discovery.AddressTypeIPv4 || eps.DeletionTimestamp != nil {
			continue
		}
		result = append(result, eps)
	}

	return result
}

func endpointSlicesForServiceByREST(k8sClientSet kubernetes.Interface, namespace, name string) (*discovery.EndpointSliceList, error) {
	esLabelSelector := labels.Set(map[string]string{
		discovery.LabelServiceName: name,
	}).AsSelectorPreValidated()

	listOpt := metav1.ListOptions{
		LabelSelector: esLabelSelector.String(),
	}
	return k8sClientSet.DiscoveryV1().EndpointSlices(namespace).List(context.TODO(), listOpt)
}

func endpointSlicesForServiceByLister(endpointSliceLister discoverylisters.EndpointSliceLister, namespace, name string) ([]*discovery.EndpointSlice, error) {
	esLabelSelector := labels.Set(map[string]string{
		discovery.LabelServiceName: name,
		discovery.LabelManagedBy:   controllerName,
	}).AsSelectorPreValidated()
	return endpointSliceLister.EndpointSlices(namespace).List(esLabelSelector)
}
