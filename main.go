package main

import (
	"flag"
	"fmt"
	"time"

	discoveryclient "k8s.io/client-go/discovery"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	clientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	sharedInformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"

	"github.com/k8snetworkplumbingwg/k8s-net-attach-def-controller/pkg/controller"
	"github.com/k8snetworkplumbingwg/k8s-net-attach-def-controller/pkg/signals"
	"github.com/k8snetworkplumbingwg/k8s-net-attach-def-controller/pkg/utils"
)

var (
	master     string
	kubeconfig string

	// defines default resync period between k8s API server and controller
	syncPeriod = time.Second * 30

	// default workers of this controller
	defaultWorkers = 3

	version bool
)

func main() {
	// initialize klog/v2, can also bind to a local flagset if desired
	klog.InitFlags(nil)

	flag.StringVar(&master, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Required if out-of-cluster.")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Required if out-of-cluster.")
	flag.BoolVar(&version, "version", false, "Show version")

	// parse custom and klog/v2 flags
	flag.Parse()

	if version {
		fmt.Printf("Version %v - %v\n", utils.VERSION, utils.COMMIT)
		return
	}

	// make sure we flush before exiting
	defer klog.Flush()

	// set up signals so we handle the first shutdown signal gracefully
	stopChan := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		klog.Fatalf("error building kubeconfig: %s", err.Error())
	}

	if !klog.V(4).Enabled() {
		cfg.WarningHandler = rest.NoWarnings{}
	}

	k8sClientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("error creating kubernetes clientset: %s", err.Error())
	}

	netAttachDefClientSet, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("error creating net-attach-def clientset: %s", err.Error())
	}

	disClient := discoveryclient.NewDiscoveryClientForConfigOrDie(cfg)

	netAttachDefInformerFactory := sharedInformers.NewSharedInformerFactory(netAttachDefClientSet, syncPeriod)
	k8sInformerFactory := informers.NewSharedInformerFactory(k8sClientSet, syncPeriod)

	networkController := controller.NewNetworkController(
		k8sClientSet,
		netAttachDefClientSet,
		disClient,
		netAttachDefInformerFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions(),
		k8sInformerFactory,
	)

	netAttachDefInformerFactory.Start(stopChan)
	k8sInformerFactory.Start(stopChan)

	networkController.Run(defaultWorkers, stopChan)
}
