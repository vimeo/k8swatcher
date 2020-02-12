//   Copyright 2020 Vimeo
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package k8swatcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	retry "github.com/vimeo/go-retry"

	k8score "k8s.io/api/core/v1"
	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// InitKubernetesInClusterClient initializes a kubernetes clientset with the
// config that service account kubernetes gives to pods. Assumes the
// application is running in a kubernetes cluster and will return an error when
// not running in cluster
func InitKubernetesInClusterClient() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	// creates a kubernetes clientset with the config initialized above
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return clientset, fmt.Errorf("Error initializing kubernetes client. Error: %s", err)
	}
	return clientset, nil
}

// InitKubernetesExtraClusterClient attempts to initialize a kubernetes client
// outside the cluster
func InitKubernetesExtraClusterClient(configPath, context string) (kubernetes.Interface, error) {
	client, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: configPath},
		&clientcmd.ConfigOverrides{CurrentContext: context},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to configure k8s client: %w", err)
	}
	return kubernetes.NewForConfig(client)
}

// PodEvent is the interface type for events being returned over a channel
type PodEvent interface {
	PodName() string
}

// EventCallback defines a function for handling pod lifetime events
// the context will be a copy of the context passed to the Run() method on
// PodWatcher.
type EventCallback func(context.Context, PodEvent)

// CreatePod indicates that a pod has been created and now has an assigned IP
type CreatePod struct {
	name string
	IP   *net.IPAddr
	// The new Pod's definition
	Def *k8score.Pod
}

// PodName implements PodEvent
func (c *CreatePod) PodName() string { return c.name }

// ModPod indicates a change in a pod's status or definition (anything that
// isn't a creation or deletion)
type ModPod struct {
	name string
	IP   *net.IPAddr
	// The new Pod definition
	Def *k8score.Pod
}

// PodName implements PodEvent
func (c *ModPod) PodName() string { return c.name }

// DeletePod indicates that a pod has been destroyed (or is shutting down) and
// should no longer be watched.
type DeletePod struct {
	name string
}

// PodName implements PodEvent
func (c *DeletePod) PodName() string { return c.name }

// PodWatcher uses the k8s API to watch for new/deleted k8s pods
type PodWatcher struct {
	cs           kubernetes.Interface
	k8sNamespace string
	selector     string

	cbs []EventCallback

	// Logger is a Logger implementation which is used for logging if non-nil
	Logger Logger
}

// Logger implementations resemble the stdlogger interface.
type Logger interface {
	Printf(string, ...interface{})
}

// NewPodWatcher constructs a new PodWatcher. Each callback in the variadic
// callback list is called in its own goroutine.
func NewPodWatcher(cs kubernetes.Interface, k8sNamespace, selector string, cbs ...EventCallback) *PodWatcher {
	return &PodWatcher{
		cs:           cs,
		k8sNamespace: k8sNamespace,
		selector:     selector,
		cbs:          cbs,
	}
}

func (p *PodWatcher) logf(format string, args ...interface{}) {
	if p.Logger == nil {
		return
	}
	p.Logger.Printf(format, args...)
}

func (p *PodWatcher) listOpts() k8smeta.ListOptions {
	return k8smeta.ListOptions{LabelSelector: p.selector}
}

// returns the number of pods, resource version and (optionally) an error
func (p *PodWatcher) initialPods(ctx context.Context) (int, string, error) {
	initPods, initListerr := p.cs.CoreV1().Pods(p.k8sNamespace).List(
		p.listOpts())
	if initListerr != nil {
		return -1, "", fmt.Errorf("failed initial pod listing: %w", initListerr)
	}
	for _, pod := range initPods.Items {
		switch pod.Status.Phase {
		// Skip the pods that already exited
		case k8score.PodFailed, k8score.PodSucceeded:
			// TODO: Track which pods we skip here so we don't
			// deliver the deletion message for them.
			continue
		case k8score.PodPending, k8score.PodRunning:
		default:
		}
		podIP := pod.Status.PodIP
		ipaddr := net.ParseIP(podIP)
		for _, cb := range p.cbs {
			cb(ctx, &CreatePod{
				name: pod.Name,
				IP:   &net.IPAddr{IP: ipaddr},
			})
		}
	}
	return len(initPods.Items), initPods.ResourceVersion, nil
}

// ErrResultsClosed indicates that the function exited because the results
// channel was closed.
var ErrResultsClosed = errors.New("k8s result channel closed")

const backoffResetThreshold = time.Hour

// Run starts a watcher loop, will generate CreatePod events for all existing
// pods at startup (those callbacks are run inband, so don't do anything
// expensive in them).
func (p *PodWatcher) Run(ctx context.Context) error {
	npods, version, initErr := p.initialPods(ctx)
	if initErr != nil {
		return initErr
	}

	cbChans := make([]chan<- PodEvent, len(p.cbs))
	wg := sync.WaitGroup{}
	wg.Add(len(p.cbs))
	for i, cb := range p.cbs {
		ch := make(chan PodEvent, 2*npods+32)
		cbChans[i] = ch
		go p.cbRunner(ctx, cb, &wg, ch)
	}
	defer wg.Wait()
	// Make sure we actually shut down our cbrunning goroutines before we
	// exit the loop.
	defer func() {
		// close the callback channels
		for _, ch := range cbChans {
			close(ch)
		}
	}()

	backoff := retry.DefaultBackoff.Clone()

	lastwatchStart := time.Now()
	for {
		watchOpt := p.listOpts()
		watchOpt.ResourceVersion = version
		podWatch, watchStartErr := p.cs.CoreV1().Pods(p.k8sNamespace).Watch(watchOpt)
		if watchStartErr != nil {
			return fmt.Errorf("failed to startup watcher: %w", watchStartErr)
		}

		rv, err := p.watch(ctx, podWatch, version, cbChans)
		if err != ErrResultsClosed {
			return err
		}
		version = rv

		// If it's been a while, reset the backoff so we don't wait too
		// long after things have been humming for an hour.
		if time.Since(lastwatchStart) > backoffResetThreshold {
			backoff.Reset()
		}

		timer := time.NewTimer(backoff.Next())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		// update lastwatchStart
		lastwatchStart = time.Now()
	}
}

func (p *PodWatcher) cbRunner(ctx context.Context, cb EventCallback, wg *sync.WaitGroup, ch <-chan PodEvent) {
	defer wg.Done()
	for ev := range ch {
		cb(ctx, ev)
	}
}

func (p *PodWatcher) watch(ctx context.Context, podWatch watch.Interface, rv string, cbChans []chan<- PodEvent) (string, error) {
	lastRV := rv
	resCh := podWatch.ResultChan()
	for {
		select {
		case <-ctx.Done():
			podWatch.Stop()
			// drain the result channel then exit
			for range resCh {
			}
			return lastRV, nil
		case ev, ok := <-resCh:
			if !ok {
				return lastRV, ErrResultsClosed
			}
			pod, ok := ev.Object.(*k8score.Pod)
			if !ok {
				if ev.Type == watch.Error {
					p.logf("received error event: %s", ev)
				}
				continue
			}
			lastRV = pod.ResourceVersion
			podName := pod.ObjectMeta.Name
			podIP := pod.Status.PodIP
			ipaddr := net.ParseIP(podIP)
			var clientEvent PodEvent
			switch ev.Type {
			case watch.Added:
				clientEvent = &CreatePod{
					name: podName,
					IP: &net.IPAddr{
						IP:   ipaddr,
						Zone: "",
					},
					Def: pod,
				}
			case watch.Modified:
				clientEvent = &ModPod{
					name: podName,
					IP: &net.IPAddr{
						IP:   ipaddr,
						Zone: "",
					},
					Def: pod,
				}
			case watch.Deleted:
				clientEvent = &DeletePod{
					name: podName,
				}
			case watch.Bookmark:
				// we're not using Bookmarks
				continue
			case watch.Error:
				// we probably won't get here since the object
				// probably won't be of type k8score.Pod
				p.logf("received error event: %s", ev)
				continue
			}

			for _, ch := range cbChans {
				ch <- clientEvent
			}
		}
	}

}
