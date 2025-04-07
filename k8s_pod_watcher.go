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
	"net/http"
	"sync"
	"time"

	retry "github.com/vimeo/go-retry"

	k8score "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
		return clientset, fmt.Errorf("error initializing kubernetes client. Error: %s", err)
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

// ResourceVersion is the version string associated with the object/event. K8s
// documents that it is only valid to compare it for equality, and hence not
// otherwise sortable.
type ResourceVersion string

// PodEvent is the interface type for events being returned over a channel
type PodEvent interface {
	PodName() string
	ResourceVersion() ResourceVersion
	// Continues indicates that the next event will use the same
	// ResourceVersion because this event is part of either the same
	// initial-state-dump or resync.
	Continues() bool
}

// EventCallback defines a function for handling pod lifetime events
// the context will be a copy of the context passed to the Run() method on
// PodWatcher.
type EventCallback func(context.Context, PodEvent)

func podIsReady(pod *k8score.Pod) bool {
	if pod.Status.Phase != k8score.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == k8score.PodReady && condition.Status == k8score.ConditionTrue {
			return true
		}
	}
	return false
}

// CreatePod indicates that a pod has been created and now has an assigned IP
type CreatePod struct {
	name string
	rv   ResourceVersion
	IP   *net.IPAddr
	// The new Pod's definition
	Def *k8score.Pod

	continues bool
}

// PodName implements PodEvent
func (c *CreatePod) PodName() string { return c.name }

// ResourceVersion implements PodEvent
func (c *CreatePod) ResourceVersion() ResourceVersion { return c.rv }

// Continues indicates that the next event will use the same
// ResourceVersion because this event is part of either the same
// initial-state-dump or resync.
func (c *CreatePod) Continues() bool { return c.continues }

// IsReady indicates whether k8s considers this pod ready (and it isn't shutting down)
func (c *CreatePod) IsReady() bool {
	return podIsReady(c.Def)
}

// ModPod indicates a change in a pod's status or definition (anything that
// isn't a creation or deletion)
type ModPod struct {
	name string
	rv   ResourceVersion
	IP   *net.IPAddr
	// The new Pod definition
	Def *k8score.Pod

	continues bool
}

// PodName implements PodEvent
func (m *ModPod) PodName() string { return m.name }

// ResourceVersion implements PodEvent
func (m *ModPod) ResourceVersion() ResourceVersion { return m.rv }

// Continues indicates that the next event will use the same
// ResourceVersion because this event is part of either the same
// initial-state-dump or resync.
func (m *ModPod) Continues() bool { return m.continues }

// IsReady indicates whether k8s considers this pod ready (and it isn't shutting down)
func (m *ModPod) IsReady() bool {
	return podIsReady(m.Def)
}

// DeletePod indicates that a pod has been destroyed (or is shutting down) and
// should no longer be watched.
type DeletePod struct {
	name string
	rv   ResourceVersion

	continues bool
}

// PodName implements PodEvent
func (d *DeletePod) PodName() string { return d.name }

// ResourceVersion implements PodEvent
func (d *DeletePod) ResourceVersion() ResourceVersion { return d.rv }

// Continues indicates that the next event will use the same
// ResourceVersion because this event is part of the same resync.
func (d *DeletePod) Continues() bool { return d.continues }

// InitialListComplete is a synthetic event indicating that the initial list of
// pods matching the specified label matcher is complete (all previous
// `CreatePod` events' callbacks have completed).
type InitialListComplete struct {
	rv ResourceVersion
}

// PodName returns an empty string because it's not really a single pod event.
func (i *InitialListComplete) PodName() string { return "" }

// ResourceVersion returns the ResourceVersion of the initial list
func (i *InitialListComplete) ResourceVersion() ResourceVersion { return i.rv }

// Continues indicates that the next event will use the same
// ResourceVersion because this event is part of either the same
// initial-state-dump or resync.
// This is always the last event of the initial state-dump, so it always has
// Continues set to false.
func (i *InitialListComplete) Continues() bool {
	return false
}

// PodWatcher uses the k8s API to watch for new/deleted k8s pods
type PodWatcher struct {
	cs           kubernetes.Interface
	k8sNamespace string
	listOpts     k8smeta.ListOptions

	cbs []EventCallback

	tracker podTracker

	// Logger is a Logger implementation which is used for logging if non-nil
	Logger Logger
}

// Logger implementations resemble the stdlogger interface.
type Logger interface {
	Printf(string, ...interface{})
}

// NewPodWatcherOptions constructs a new PodWatcher with a potentially more
// complicated selector. Each callback in the variadic callback list is called
// in its own goroutine.
func NewPodWatcherOptions(cs kubernetes.Interface, k8sNamespace string, options k8smeta.ListOptions, cbs ...EventCallback) *PodWatcher {
	return &PodWatcher{
		cs:           cs,
		k8sNamespace: k8sNamespace,
		listOpts:     options,
		cbs:          cbs,
		tracker: podTracker{
			lastStatus:  map[string]*k8score.Pod{},
			lastVersion: "",
		},
	}
}

// NewPodWatcher constructs a new PodWatcher. The `selector` selects by labels
// only.  Each callback in the variadic callback list is called in its own
// goroutine.
func NewPodWatcher(cs kubernetes.Interface, k8sNamespace, selector string, cbs ...EventCallback) *PodWatcher {
	return NewPodWatcherOptions(cs, k8sNamespace, k8smeta.ListOptions{LabelSelector: selector}, cbs...)
}

func (p *PodWatcher) logf(format string, args ...interface{}) {
	if p.Logger == nil {
		return
	}
	p.Logger.Printf(format, args...)
}

func setContinues(ev PodEvent, continues bool) {
	switch pe := ev.(type) {
	case *CreatePod:
		pe.continues = continues
	case *ModPod:
		pe.continues = continues
	case *DeletePod:
		pe.continues = continues
	default:
		panic(fmt.Errorf("unhandled type %T", ev))
	}
}

// returns the new version ID (or an error)
func (p *PodWatcher) resync(ctx context.Context, cbChans []chan<- PodEvent) (string, error) {
	initPods, initListerr := p.cs.CoreV1().Pods(p.k8sNamespace).List(
		ctx, p.listOpts)
	if initListerr != nil {
		return "", fmt.Errorf("failed pod listing: %w", initListerr)
	}
	podNames := make([]string, len(initPods.Items))

	synthPodEvents := make([]PodEvent, 0, len(initPods.Items))
	for i, pod := range initPods.Items {
		podNames[i] = pod.Name
		if ev := p.tracker.synthesizeEvent(&initPods.Items[i]); ev != nil {
			synthPodEvents = append(synthPodEvents, ev)
		}
	}

	// We need to know how many pods died in order to set continues properly on the non-delete
	// events, but we can't update the tracker state until after we've synthesized all the other
	// events.
	deadPods := p.tracker.findRemoveDeadPods(podNames)

	for i, ev := range synthPodEvents {
		// All events other than the last one (possibly a DeletePod) must set continues to
		// true so the client can tell when it has a consistent view for the relevant
		// ResourceVersion.
		continues := len(deadPods) > 0 || i < len(synthPodEvents)-1
		setContinues(ev, continues)
		for _, ch := range cbChans {
			ch <- ev
		}
	}

	bodyCount := 0
	for podName := range deadPods {
		bodyCount++
		for _, ch := range cbChans {
			ch <- &DeletePod{
				name:      podName,
				rv:        ResourceVersion(initPods.ResourceVersion),
				continues: bodyCount < len(deadPods),
			}
		}
	}
	return initPods.ResourceVersion, nil
}

// returns the number of pods, resource version and (optionally) an error
func (p *PodWatcher) initialPods(ctx context.Context) (int, string, error) {
	initPods, initListerr := p.cs.CoreV1().Pods(p.k8sNamespace).List(
		ctx, p.listOpts)
	if initListerr != nil {
		return -1, "", fmt.Errorf("failed initial pod listing: %w", initListerr)
	}
	for i, pod := range initPods.Items {
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
		event := CreatePod{
			name: pod.Name,
			rv:   ResourceVersion(initPods.ResourceVersion),
			IP:   &net.IPAddr{IP: ipaddr},
			// be sure NOT to use the loop variable here :-)
			Def: &initPods.Items[i],
			// only the last event sets continues to false (and
			// that'll be the InitialListComplete event)
			continues: true,
		}
		p.tracker.recordEvent(&event)
		for _, cb := range p.cbs {
			cb(ctx, &event)
		}
	}
	// Inform all the callbacks that the full-state dump is complete.
	for _, cb := range p.cbs {
		cb(ctx, &InitialListComplete{rv: ResourceVersion(initPods.ResourceVersion)})
	}
	return len(initPods.Items), initPods.ResourceVersion, nil
}

// ErrResultsClosed indicates that the function exited because the results
// channel was closed.
var ErrResultsClosed = errors.New("k8s result channel closed")

var errVersionGone = errors.New("k8s GONE status; resync required")

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

	backoff := retry.DefaultBackoff()

	// Returns true if the caller should return, false otherwise
	sleepBackoff := func() bool {
		timer := time.NewTimer(backoff.Next())
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return true
		case <-timer.C:
		}
		return false
	}

	type resyncAction uint8
	const (
		resyncReturn resyncAction = iota
		resyncContinueLoop
		resyncSuccess
	)

	// returns whether to keep looping
	resync := func() resyncAction {
		newversion, resyncErr := p.resync(ctx, cbChans)
		if resyncErr != nil {
			p.logf("resync failed: %s", resyncErr)
			if sleepBackoff() {
				return resyncReturn
			}
			return resyncContinueLoop
		}
		p.logf("resync succeeded; new version: %q (old %q)", newversion, version)
		version = newversion
		return resyncSuccess
	}

	lastwatchStart := time.Now()
	for {
		watchOpt := p.listOpts
		watchOpt.ResourceVersion = version
		podWatch, watchStartErr := p.cs.CoreV1().Pods(p.k8sNamespace).Watch(ctx, watchOpt)
		if watchStartErr != nil {
			switch t := watchStartErr.(type) {
			case k8serrors.APIStatus:
				if t.Status().Code != http.StatusGone {
					return fmt.Errorf("failed to startup watcher with status code %d: %w",
						t.Status().Code, watchStartErr)
				}
			default:
				noe := (*net.OpError)(nil)
				if errors.As(watchStartErr, &noe) {
					// If it's a connection error of some sort, we want to backoff and retry
					if sleepBackoff() {
						return nil
					}
					continue
				}
				return fmt.Errorf("failed to startup watcher: %w", watchStartErr)
			}
			switch resync() {
			case resyncReturn:
				return nil
			case resyncContinueLoop:
				continue
			case resyncSuccess:
			}
		}

		rv, err := p.watch(ctx, podWatch, version, cbChans)
		switch err {
		case ErrResultsClosed:
			version = rv
		case errVersionGone:
			switch resync() {
			case resyncReturn:
				return nil
			case resyncContinueLoop:
				continue
			case resyncSuccess:
			}
		default:
			return err
		}

		// If it's been a while, reset the backoff so we don't wait too
		// long after things have been humming for an hour.
		if time.Since(lastwatchStart) > backoffResetThreshold {
			backoff.Reset()
		}

		if sleepBackoff() {
			return nil
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

func errorEventIsGone(ev watch.Event) bool {
	errObj, isStatus := ev.Object.(*k8smeta.Status)
	if !isStatus {
		return false
	}
	return errObj.Code == http.StatusGone
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
					// if the error has status code "Gone" (410),
					// return and let the outer loop
					// reconnect.
					if errorEventIsGone(ev) {
						podWatch.Stop()
						// drain the result channel then exit
						for range resCh {
						}
						return lastRV, errVersionGone
					}
				}
				continue
			}
			lastRV = pod.ResourceVersion
			podName := pod.Name
			podIP := pod.Status.PodIP
			ipaddr := net.ParseIP(podIP)
			var clientEvent PodEvent
			switch ev.Type {
			case watch.Added:
				clientEvent = &CreatePod{
					name: podName,
					rv:   ResourceVersion(lastRV),
					IP: &net.IPAddr{
						IP:   ipaddr,
						Zone: "",
					},
					Def:       pod,
					continues: false,
				}
			case watch.Modified:
				clientEvent = &ModPod{
					name: podName,
					rv:   ResourceVersion(lastRV),
					IP: &net.IPAddr{
						IP:   ipaddr,
						Zone: "",
					},
					Def:       pod,
					continues: false,
				}
			case watch.Deleted:
				clientEvent = &DeletePod{
					name:      podName,
					rv:        ResourceVersion(lastRV),
					continues: false,
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

			// before sending these events to the client callbacks,
			// update our internal state-tracker.
			p.tracker.recordEvent(clientEvent)

			for _, ch := range cbChans {
				ch <- clientEvent
			}
		}
	}

}
