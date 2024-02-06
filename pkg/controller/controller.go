package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	discoveryclient "k8s.io/client-go/discovery"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/v1/endpoints"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"

	"gopkg.in/intel/multus-cni.v3/types"

	netattachdef "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	clientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	nadinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions/k8s.cni.cncf.io/v1"
)

const (
	selectionsKey            = "k8s.v1.cni.cncf.io/networks"
	statusesKey              = "k8s.v1.cni.cncf.io/networks-status"
	controllerName           = "net-attach-def.panda.io"
	svcSuffixMacvlan         = "-macvlan"
	svcPrefixIngress         = "ingress-"
	enableEndpointSliceWatch = "PANDA_ENABLE_ENDPOINTSLICE_WATCH"
)

// NetworkController is the controller implementation for handling net-attach-def resources and other objects using them
type NetworkController struct {
	k8sClientSet          kubernetes.Interface
	netAttachDefClientSet clientset.Interface

	netAttachDefsSynced cache.InformerSynced

	podsLister corelisters.PodLister
	podsSynced cache.InformerSynced

	serviceLister  corelisters.ServiceLister
	servicesSynced cache.InformerSynced

	endpointsLister corelisters.EndpointsLister
	endpointsSynced cache.InformerSynced

	endpointSliceLister  discoverylisters.EndpointSliceLister
	endpointSlicesSynced cache.InformerSynced

	workqueue workqueue.RateLimitingInterface

	recorder record.EventRecorder

	needToUpdateEndpointSlice bool
}

// NewNetworkController returns new NetworkController instance
func NewNetworkController(
	k8sClientSet kubernetes.Interface,
	netAttachDefClientSet clientset.Interface,
	disClient *discoveryclient.DiscoveryClient,
	netAttachDefInformer nadinformers.NetworkAttachmentDefinitionInformer,
	k8sInformerFactory informers.SharedInformerFactory) *NetworkController {

	serviceInformer := k8sInformerFactory.Core().V1().Services()
	podInformer := k8sInformerFactory.Core().V1().Pods()
	endpointInformer := k8sInformerFactory.Core().V1().Endpoints()

	klog.V(3).Info("creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.V(4).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: k8sClientSet.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerName})

	rateLimiter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemFastSlowRateLimiter(time.Millisecond, 2*time.Minute, 30),
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 30*time.Second),
	)

	nc := &NetworkController{
		k8sClientSet:          k8sClientSet,
		netAttachDefClientSet: netAttachDefClientSet,
		netAttachDefsSynced:   netAttachDefInformer.Informer().HasSynced,
		servicesSynced:        serviceInformer.Informer().HasSynced,
		podsSynced:            podInformer.Informer().HasSynced,
		endpointsSynced:       endpointInformer.Informer().HasSynced,
		serviceLister:         serviceInformer.Lister(),
		podsLister:            podInformer.Lister(),
		endpointsLister:       endpointInformer.Lister(),
		workqueue:             workqueue.NewNamedRateLimitingQueue(rateLimiter, "secondary_endpoints"),
		recorder:              recorder,
	}

	err := discoveryclient.ServerSupportsVersion(disClient, discovery.SchemeGroupVersion)
	if err == nil {
		endpointSliceInformer := k8sInformerFactory.Discovery().V1().EndpointSlices()
		nc.endpointSliceLister = endpointSliceInformer.Lister()
		nc.endpointSlicesSynced = endpointSliceInformer.Informer().HasSynced
		nc.needToUpdateEndpointSlice = true

		/* setup handlers for endpointslice events */
		if strings.EqualFold(os.Getenv(enableEndpointSliceWatch), "true") {
			endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: nc.handleEndpointSliceEvent,
				UpdateFunc: func(old, updated interface{}) {
					if objectChanged(old, updated) {
						nc.handleEndpointSliceEvent(updated)
					}
				},
			})
		}
	}

	netAttachDefInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: nc.handleNetAttachDefDeleteEvent,
	})

	/* setup handlers for endpoints events */
	endpointInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nc.handleEndpointEvent,
		UpdateFunc: func(old, updated interface{}) {
			if objectChanged(old, updated) {
				nc.handleEndpointEvent(updated)
			}
		},
		DeleteFunc: nc.handleEndpointEvent,
	})

	/* setup handlers for services events */
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nc.handleServiceEvent,
		UpdateFunc: func(old, updated interface{}) {
			if objectChanged(old, updated) || networkAnnotationsChanged(old, updated) {
				nc.handleServiceEvent(updated)
			}
		},
		DeleteFunc: nc.handleServiceEvent,
	})

	/* setup handlers for pods events */
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: nc.handlePodEvent,
		UpdateFunc: func(old, updated interface{}) {
			if objectChanged(old, updated) {
				nc.handlePodEvent(updated)
			}
		},
		DeleteFunc: nc.handlePodEvent,
	})

	return nc
}

func (c *NetworkController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *NetworkController) processNextWorkItem() bool {
	obj, shouldQuit := c.workqueue.Get()

	if shouldQuit {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		_, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
			return nil
		}
		if !c.needToSyncService(name) {
			klog.V(4).Infof("ignore syncing service %q", key)
			return nil
		}

		if err := c.sync(key); err != nil {
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced key '%s'", key)
		return nil
	}(obj)

	if err != nil {
		klog.V(4).Infof("sync aborted: %s", err)
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *NetworkController) sync(key string) error {
	// get service object from the key
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	svc, err := c.serviceLister.Services(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("service '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	// read network annotations from the service
	annotations := getNetworkAnnotations(svc)
	if len(annotations) == 0 {
		return nil
	}
	klog.V(3).Infof("service network annotation found: %v", annotations)
	networks, err := parsePodNetworkSelections(annotations, svc.Namespace)
	if err != nil {
		klog.Errorf("service network annotation parse error: %v", err)
		return nil
	}
	if len(networks) > 1 {
		msg := fmt.Sprintf("multiple network selections in the service spec are not supported")
		klog.Warningf(msg)
		c.recorder.Event(svc, corev1.EventTypeWarning, msg, "Endpoints update aborted")
		return nil
	}

	// get pods matching service selector
	selector := labels.Set(svc.Spec.Selector).AsSelector()
	pods, err := c.podsLister.List(selector)
	if err != nil {
		// no selector or no pods running
		klog.Infof("error listing pods matching service selector: %s", err)
		return err
	}

	// get endpoints of the service
	ep, err := c.endpointsLister.Endpoints(svc.Namespace).Get(svc.Name)
	if err != nil {
		klog.Infof("error getting service endpoints: %s", err)
		return err
	}

	subsets := make([]corev1.EndpointSubset, 0)
	epsForEndpointSlice := make([]discovery.Endpoint, 0)
	epPortsForEndpointSlice := make([]discovery.EndpointPort, 0)

	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		addresses := make([]corev1.EndpointAddress, 0)
		ports := make([]corev1.EndpointPort, 0)

		networksStatus := make([]types.NetworkStatus, 0)
		err := json.Unmarshal([]byte(pod.Annotations[statusesKey]), &networksStatus)
		if err != nil {
			klog.Warningf("skip to update for pod %s as networks status are not expected: %v", pod.Name, err)
			continue
		}
		// find networks used by pod and match network annotation of the service
		for _, status := range networksStatus {
			if isInNetworkSelectionElementsArray(status.Name, pod.Namespace, networks) {
				klog.V(3).Infof("processing pod %s/%s: found network %s interface %s with IP addresses %s",
					pod.Namespace, pod.Name, annotations, status.Interface, status.IPs)
				// all IPs of matching network are added as endpoints
				for _, ip := range status.IPs {
					epAddress := corev1.EndpointAddress{
						IP:       ip,
						NodeName: &pod.Spec.NodeName,
						TargetRef: &corev1.ObjectReference{
							Kind:            "Pod",
							Name:            pod.GetName(),
							Namespace:       pod.GetNamespace(),
							ResourceVersion: pod.GetResourceVersion(),
							UID:             pod.GetUID(),
						},
					}
					addresses = append(addresses, epAddress)

					esAddress := addressToEndpoint(epAddress)
					epsForEndpointSlice = append(epsForEndpointSlice, esAddress)
				}
			}
		}
		for i := range svc.Spec.Ports {
			// check whether pod has the ports needed by service and add them to endpoints if so
			portNumber, err := podutil.FindPort(pod, &svc.Spec.Ports[i])
			if err != nil {
				klog.Infof("Could not find pod port for service %s/%s: %s, skipping...", svc.Namespace, svc.Name, err)
				continue
			}

			port := corev1.EndpointPort{
				Port:     int32(portNumber),
				Protocol: svc.Spec.Ports[i].Protocol,
				Name:     svc.Spec.Ports[i].Name,
			}
			ports = append(ports, port)
		}
		subset := corev1.EndpointSubset{
			Addresses: addresses,
			Ports:     ports,
		}
		subsets = append(subsets, subset)
	}

	var updatedEndpoint *corev1.Endpoints
	// update endpoints resource
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := c.k8sClientSet.CoreV1().Endpoints(ep.Namespace).Get(context.TODO(), ep.Name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to get latest version of endpoints: %v", err)
			return err
		}

		// repack subsets - NOTE: too naive? additional checks needed?
		toUpdateSubsets := endpoints.RepackSubsets(subsets)
		// to update ports for endpointslice
		for _, subset := range toUpdateSubsets {
			epPorts := epPortsToEpsPorts(subset.Ports)
			epPortsForEndpointSlice = append(epPortsForEndpointSlice, epPorts...)
		}

		// check if need to call an update
		if apiequality.Semantic.DeepDerivative(toUpdateSubsets, result.Subsets) {
			klog.Infof("skip to update endpoints %s as semantic deep derivative", result.Name)
			return nil
		}

		resultCopy := result.DeepCopy()

		resultCopy.SetOwnerReferences(
			[]metav1.OwnerReference{
				*metav1.NewControllerRef(svc, schema.GroupVersionKind{
					Group:   corev1.SchemeGroupVersion.Group,
					Version: corev1.SchemeGroupVersion.Version,
					Kind:    "Service",
				}),
			},
		)

		if resultCopy.Labels == nil {
			resultCopy.Labels = map[string]string{}
		}
		resultCopy.Labels[discovery.LabelSkipMirror] = "true"

		resultCopy.Subsets = toUpdateSubsets
		updatedEndpoint, err = c.k8sClientSet.CoreV1().Endpoints(ep.Namespace).Update(context.TODO(), resultCopy, metav1.UpdateOptions{})
		return err
	})
	if retryErr != nil {
		klog.Errorf("endpoint update error: %v", retryErr)
		return retryErr
	}

	msg := fmt.Sprintf("Updated to use network %s", annotations)
	if updatedEndpoint != nil {
		klog.V(3).Info("endpoint updated successfully")
		c.recorder.Event(ep, corev1.EventTypeNormal, msg, "Endpoints update successful")
		c.recorder.Event(svc, corev1.EventTypeNormal, msg, "Endpoints update successful")
	}

	if !c.needToUpdateEndpointSlice {
		klog.V(4).Info("no need to update endpointslice as k8s is not support discovery.k8s.io/v1")
		return nil
	}

	endpointSliceUpdated := false
	sortEpsEndpoints(epsForEndpointSlice)
	sortEpsPorts(epPortsForEndpointSlice)
	klog.V(3).Infof("trying to update endpointslice with %#v", epsForEndpointSlice)
	retryErr = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		endpointSlices, err := endpointSlicesForServiceByREST(c.k8sClientSet, svc.Namespace, svc.Name)
		if err != nil {
			klog.Errorf("endpointslice list error: %v", err)
			return err
		}

		toActionList := filterEpsList(endpointSlices)

		for _, endpointSlice := range toActionList {
			esCopy := endpointSlice.DeepCopy()
			epsCopy := esCopy.Endpoints
			portsCopy := esCopy.Ports
			sortEpsEndpoints(epsCopy)
			sortEpsPorts(portsCopy)
			klog.V(4).Infof("### Endpoint copy: %#v", epsCopy)
			klog.V(4).Infof("### Endpoint compared: %t", apiequality.Semantic.DeepDerivative(epsForEndpointSlice, epsCopy))
			klog.V(4).Infof("### EndpointPort length %d ---- %d", len(portsCopy), len(epPortsForEndpointSlice))
			klog.V(4).Infof("### EndpointPort compared %t", apiequality.Semantic.DeepDerivative(epPortsForEndpointSlice, portsCopy))
			if len(esCopy.Endpoints) == len(epsForEndpointSlice) &&
				apiequality.Semantic.DeepDerivative(epsForEndpointSlice, epsCopy) &&
				apiequality.Semantic.DeepDerivative(epPortsForEndpointSlice, portsCopy) {
				klog.Infof("skip to update endpointslice %s as semantic deep derivative", esCopy.Name)
				continue
			}
			esCopy.Labels[discovery.LabelManagedBy] = controllerName
			esCopy.Endpoints = epsForEndpointSlice
			esCopy.Ports = epPortsForEndpointSlice
			_, err = c.k8sClientSet.DiscoveryV1().EndpointSlices(esCopy.Namespace).Update(context.TODO(),
				esCopy,
				metav1.UpdateOptions{})
			if err != nil {
				klog.Errorf("endpointslice update error: %v", err)
				return err
			}
			c.recorder.Event(esCopy, corev1.EventTypeNormal, msg, "EndpointSlices update successful")
			endpointSliceUpdated = true
		}

		if len(toActionList) > 1 {
			for _, endpointSlice := range toActionList[1:] {
				err := c.k8sClientSet.DiscoveryV1().EndpointSlices(endpointSlice.Namespace).Delete(context.TODO(),
					endpointSlice.Name, metav1.DeleteOptions{})
				if err != nil {
					klog.Errorf("endpointslice delete error: %v", err)
					continue
				}
				klog.Infof("deleted endpointslice %s", endpointSlice.Name)
			}
		}
		return nil
	})
	if retryErr != nil {
		klog.Errorf("endpointslice update error: %v", retryErr)
		return retryErr
	}

	if endpointSliceUpdated {
		klog.V(3).Info("endpointslice updated successfully")
		c.recorder.Event(svc, corev1.EventTypeNormal, msg, "EndpointSlices update successful")
	}

	return nil
}

func (c *NetworkController) handleServiceEvent(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

func (c *NetworkController) handlePodEvent(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	// if no network annotation discard
	_, ok = pod.GetAnnotations()[selectionsKey]
	if !ok {
		klog.V(4).Info("skipping pod event: network annotations missing")
		return
	}

	// if not behind any service discard
	services, err := helper.GetPodServices(c.serviceLister, pod)
	if err != nil {
		klog.V(4).Info("skipping pod event: %s", err)
		return
	}
	for _, svc := range services {
		c.handleServiceEvent(svc)
	}
}

func (c *NetworkController) handleEndpointEvent(obj interface{}) {
	ep := obj.(*corev1.Endpoints)

	// get service associated with endpoints instance
	svc, err := c.serviceLister.Services(ep.GetNamespace()).Get(ep.GetName())
	if err != nil {
		// errors are returned for service-less endpoints such as kube-scheduler and kube-controller-manager
		return
	}

	c.handleServiceEvent(svc)
}

func (c *NetworkController) handleNetAttachDefDeleteEvent(obj interface{}) {
	klog.V(3).Info("net-attach-def delete event received")
	netAttachDef, ok := obj.(metav1.Object)
	if ok {
		name := netAttachDef.GetName()
		namespace := netAttachDef.GetNamespace()
		klog.Infof("handling deletion of %s/%s", namespace, name)
		/* NOTE: try to do something smarter - searching in pods based on the annotation if possible? */
		pods, _ := c.podsLister.Pods("").List(labels.Everything())
		/* check whether net-attach-def requested to be removed is still in use by any of the pods */
		for _, pod := range pods {
			netAnnotations, ok := pod.ObjectMeta.Annotations[selectionsKey]
			if !ok {
				continue
			}
			podNetworks, err := parsePodNetworkSelections(netAnnotations, pod.ObjectMeta.Namespace)
			if err != nil {
				continue
			}
			for _, net := range podNetworks {
				if net.Namespace == namespace && net.Name == name {
					klog.Infof("pod %s uses net-attach-def %s/%s which needs to be recreated\n", pod.ObjectMeta.Name, namespace, name)
					/* check whether the object somehow still exists */
					_, err := c.netAttachDefClientSet.K8sCniCncfIoV1().
						NetworkAttachmentDefinitions(netAttachDef.GetNamespace()).
						Get(context.TODO(), netAttachDef.GetName(), metav1.GetOptions{})
					if err != nil {
						/* recover deleted object */
						recovered := obj.(*netattachdef.NetworkAttachmentDefinition).DeepCopy()
						recovered.ObjectMeta.ResourceVersion = "" // ResourceVersion field needs to be cleared before recreating the object
						_, err = c.netAttachDefClientSet.
							K8sCniCncfIoV1().
							NetworkAttachmentDefinitions(netAttachDef.GetNamespace()).
							Create(context.TODO(), recovered, metav1.CreateOptions{})
						if err != nil {
							klog.Errorf("error recreating recovered object: %s", err.Error())
						}
						klog.V(4).Infof("net-attach-def recovered: %v", recovered)
						return
					}
				}
			}
		}
	}
}

func (c *NetworkController) handleEndpointSliceEvent(obj interface{}) {
	endpointSlice := obj.(*discovery.EndpointSlice)
	if endpointSlice == nil || endpointSlice.Labels == nil {
		return
	}
	svcName, ok := endpointSlice.Labels[discovery.LabelServiceName]
	if !ok || svcName == "" {
		return
	}
	key := fmt.Sprintf("%s/%s", endpointSlice.Namespace, svcName)
	c.workqueue.AddRateLimited(key)
}

func (c *NetworkController) needToSyncService(serviceName string) bool {
	return strings.HasSuffix(serviceName, svcSuffixMacvlan) || strings.HasPrefix(serviceName, svcPrefixIngress)
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *NetworkController) Run(workers int, stopChan <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.V(4).Infof("starting network controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	cacheSyncs := []cache.InformerSynced{
		c.netAttachDefsSynced,
		c.endpointsSynced,
		c.servicesSynced,
		c.podsSynced,
	}
	if c.endpointSlicesSynced != nil {
		cacheSyncs = append(cacheSyncs, c.endpointSlicesSynced)
	}
	if ok := cache.WaitForCacheSync(stopChan, cacheSyncs...); !ok {
		klog.Fatalf("failed waiting for caches to sync")
	}
	klog.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopChan)
	}

	klog.Info("Started workers")
	<-stopChan
	klog.V(4).Infof("shutting down network controller")
	return
}
