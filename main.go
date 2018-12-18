package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// Controller object
type Controller struct {
	queue    workqueue.RateLimitingInterface
	informer cache.SharedIndexInformer
}

// NewController creates a new controller and returns a pointer to that controller
func NewController(queue workqueue.RateLimitingInterface, informer cache.SharedIndexInformer) *Controller {
	return &Controller{
		informer: informer,
		queue:    queue,
	}
}

// Run the controller
func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// shutdown the worker when we're done
	defer c.queue.ShutDown()

	fmt.Println("starting our controller")

	// starts a shared informer
	go c.informer.Run(stopCh)

	// the workqueue can handle notifications from cache
	// when should the controller start workers processing the workqueue?
	// Listing all the resources will be inaccurate until the cache has finished synchronising.
	// Multiple rapid updates to a single resource will be collapsed into the latest version by the cache/queue
	// wait for all involved caches to be synchronized
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(errors.New("timed out waiting for caches to be synced"))
	}

	fmt.Println("cached are synchronized!")

	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second*1, stopCh)
	}

	<-stopCh
	fmt.Println("stopping our controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	log.Println(key.(string), quit)
	if quit {
		return false
	}

	defer c.queue.Done(key)

	err := c.syncToStdout(key.(string))
	c.handleErr(err, key)

	return true
}

// syncToStdout is the business logic of the controller
func (c *Controller) syncToStdout(key string) error {
	object, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		fmt.Printf("fetching object with key %s from store failed with %v\n", key, err)
		return err
	}

	if !exists {
		fmt.Printf("pod %v doesn't exist anymore\n", key)
	} else {
		fmt.Printf("Sync/Add/Update for pod %v\n", object.(*corev1.Pod).GetName())
	}

	return nil
}

func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < 5 {
		fmt.Printf("error syncing pod %v: %v\n", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	runtime.HandleError(err)
	fmt.Printf("Dropping pod %v out of the queue: %v\n", key, err)
}

func main() {

	masterURL := ""
	kubeconfigPath := filepath.Join(os.Getenv("HOME"), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
	if err != nil {
		log.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// create queue
	// The SharedInformer can't track where each controller is up to (because it's shared), so the controller must provide its own queuing and retrying mechanism
	// Hence, most Resource Event Handlers simply place items onto a per-consumer workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// create pod listwatcher
	// Listwatcher is a combination of a list function and a watch function for a specific resource in a specific namespace
	// This helps the controller focus only on the particular resource that it wants to look at
	// The field selector is a type of filter which narrows down the result of searching a resource like the controller wants to retrieve resource matching a specific field
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", corev1.NamespaceAll, fields.Everything())

	// create shared informer
	// The informer creates a local cache of a set of resources only used by itself.
	// but there is a bundle of controllers running and caring about multiple kinds of resources
	// the SharedInformer helps to create a single shared cache among controllers.
	// This means cached resources won't be duplicated and by doing that, the memory overhead of the system is reduced.
	// each SharedInformer only creates a single watch on the upstream server, regardless of how many downstream consumers are reading events from the informer.
	// reduces the load on the upstream server
	informer := cache.NewSharedIndexInformer(podListWatcher, &corev1.Pod{}, 0, cache.Indexers{})

	// the controller must manage the events that happen to the pods it's watching. To do that, SharedInformer provides us with the AddEventHandler function
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	})

	controller := NewController(queue, informer)

	// start the controller
	stop := make(chan struct{})
	defer close(stop)

	go controller.Run(1, stop)

	// wait forever
	select {}
}
