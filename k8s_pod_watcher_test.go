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
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	k8score "k8s.io/api/core/v1"
	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	testcore "k8s.io/client-go/testing"
)

const defaultNamespace = "default"

func genPod(podName, podIP string, labels map[string]string, ready bool, phase k8score.PodPhase) *k8score.Pod {
	podReady := k8score.ConditionTrue
	if !ready {
		podReady = k8score.ConditionFalse
	}
	return &k8score.Pod{
		TypeMeta: k8smeta.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: k8smeta.ObjectMeta{
			Labels:    labels,
			Name:      podName,
			Namespace: defaultNamespace,
		},
		Status: k8score.PodStatus{
			Conditions: []k8score.PodCondition{
				{
					Type:   k8score.PodReady,
					Status: podReady,
				}},
			PodIP: podIP,
			Phase: phase,
		},
	}
}

type tbWrap struct {
	tb testing.TB
}

func (tb *tbWrap) Printf(format string, args ...interface{}) {
	tb.tb.Logf(format, args...)
}

func TestListInitialPods(t *testing.T) {
	type podInfo struct {
		name, ip string
		labels   map[string]string
		ready    bool
		phase    k8score.PodPhase
	}
	for _, itbl := range []struct {
		name string
		pods []podInfo
	}{
		{
			name: "one_ready",
			pods: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}},
		},
		{
			name: "two_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
			},
		},
		{
			name: "two_not_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
		},
		{
			name: "two_not_ready_two_not_running",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar3", ip: "10.42.41.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodSucceeded},
				{name: "foobar4", ip: "10.42.44.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodSucceeded},
			},
		},
	} {
		tbl := itbl
		t.Run(tbl.name, func(t *testing.T) {
			initPods := make([]*k8score.Pod, len(tbl.pods))
			for z, pi := range tbl.pods {
				initPods[z] = genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
			}

			objs := make([]runtime.Object, len(initPods))
			for i, p := range initPods {
				objs[i] = p
			}

			cs := fake.NewSimpleClientset(objs...)

			type payload struct {
				name     string
				IP       *net.IPAddr
				isCreate bool
			}

			seenPodNames := map[string]payload{}
			evCh := make(chan payload, 1)
			cb := func(ctx context.Context, ev PodEvent) {
				var ip *net.IPAddr
				cr, isCreate := ev.(*CreatePod)
				if isCreate {
					ip = cr.IP
					if cr.Def == nil {
						t.Errorf("unexpectedly nil pod Definition in create: %v",
							ev)
					}
				}

				evCh <- payload{
					name:     ev.PodName(),
					IP:       ip,
					isCreate: isCreate,
				}

			}
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ev := range evCh {
					seenPodNames[ev.name] = ev
				}
			}()

			p := NewPodWatcher(cs, defaultNamespace, "app=fimbat", cb)
			p.Logger = &tbWrap{tb: t}
			npods, vers, err := p.initialPods(context.Background())
			if err != nil {
				t.Fatalf("failed to pull initial set of pods: %s", err)
			}

			if npods != len(tbl.pods) {
				t.Errorf("unexpected pod-count: %d, expected %d", npods, len(tbl.pods))
			}

			t.Logf("got %q version", vers)

			close(evCh)
			wg.Wait()

			for _, pi := range tbl.pods {
				pe, ok := seenPodNames[pi.name]
				if ok && pi.phase != k8score.PodRunning {
					t.Errorf("pod %q present, but not running (status %s)", pi.name, pi.phase)
					continue
				} else if pi.phase != k8score.PodRunning {
					continue
				}
				if !ok && pi.ip != "" && pi.phase == k8score.PodRunning {
					t.Errorf("pod %q has IP and is running but was not present in initial set", pi.name)
					continue
				}
				if !ok {
					t.Errorf("missing value for pod %s", pi.name)
					continue
				}
				if pe.IP.String() != pi.ip {
					t.Errorf("mismatched ip for pod %q: in: %s, out: %s", pi.name, pi.ip, pe.IP)
				}
				if !pe.isCreate {
					t.Errorf("isCreate false on initial-creation event for pod: %s", pi.name)
				}
			}

		})
	}
}
func TestListWatchPods(t *testing.T) {
	type podInfo struct {
		name, ip string
		labels   map[string]string
		ready    bool
		phase    k8score.PodPhase
	}
	for _, itbl := range []struct {
		name           string
		pods           []podInfo
		deletePods     []string
		newPods        []podInfo
		changePods     []podInfo
		expectedEvents int
	}{
		{
			name: "one_ready",
			pods: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}},
			expectedEvents: 1,
		},
		{
			name: "two_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
			},
			expectedEvents: 2,
		},
		{
			name: "two_not_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			expectedEvents: 2,
		},
		{
			name: "two_not_ready_two_not_running",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar3", ip: "10.42.41.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodSucceeded},
				{name: "foobar4", ip: "10.42.44.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodSucceeded},
			},
			expectedEvents: 2,
		},
		{
			name: "two_not_ready_one_dies",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			deletePods:     []string{"foobar2"},
			expectedEvents: 3,
		},
		{
			name: "two_not_ready_one_dies_one_create",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			deletePods: []string{"foobar2"},
			newPods: []podInfo{
				{name: "foobar3", ip: "10.42.47.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			expectedEvents: 4,
		},
		{
			name: "one_not_ready_one_pending_one_dies_one_create",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodPending},
			},
			deletePods: []string{"foobar2"},
			newPods: []podInfo{
				{name: "foobar3", ip: "10.42.47.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			changePods: []podInfo{
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},

			expectedEvents: 5,
		},
	} {
		tbl := itbl
		t.Run(tbl.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			initPods := make([]*k8score.Pod, len(tbl.pods))
			initPodMap := make(map[string]*k8score.Pod, len(tbl.pods))
			for z, pi := range tbl.pods {
				initPods[z] = genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
				initPodMap[pi.name] = initPods[z]
			}

			objs := make([]runtime.Object, len(initPods))
			for i, p := range initPods {
				objs[i] = p
			}

			cs := fake.NewSimpleClientset(objs...)

			watcher := watch.NewRaceFreeFake()
			cs.PrependWatchReactor("pods", testcore.DefaultWatchReactor(watcher, nil))

			type payload struct {
				name     string
				IP       *net.IPAddr
				isCreate bool
				isMod    bool
				isDel    bool
				pod      *k8score.Pod
			}

			evWG := sync.WaitGroup{}
			evWG.Add(tbl.expectedEvents)
			nonDeleteEvWG := sync.WaitGroup{}
			nonDeleteEvWG.Add(tbl.expectedEvents - len(tbl.deletePods))

			firstEventSeen := make(chan struct{})
			seenPodNames := map[string][]payload{}
			evCh := make(chan payload, 100)
			cb := func(ctx context.Context, ev PodEvent) {
				t.Logf("received event type %T: %+[1]v", ev)
				var ip *net.IPAddr
				var pod *k8score.Pod

				isCreate := false
				isMod := false
				isDel := false
				switch cr := ev.(type) {
				case *CreatePod:
					isCreate = true
					ip = cr.IP
					pod = cr.Def
					if pod == nil {
						t.Errorf("unexpectedly nil pod Definition in create: %v",
							ev)
					}
				case *ModPod:
					isMod = true
					ip = cr.IP
					pod = cr.Def
					if pod == nil {
						t.Errorf("unexpectedly nil pod Definition in create: %v",
							ev)
					}
				case *DeletePod:
					isDel = true
				}

				// close the firstEventSeen channel if it's not
				// already closed.
				select {
				case <-firstEventSeen:
				default:
					close(firstEventSeen)
				}

				evCh <- payload{
					name:     ev.PodName(),
					IP:       ip,
					isCreate: isCreate,
					isMod:    isMod,
					isDel:    isDel,
					pod:      pod,
				}
				evWG.Add(-1)
				if !isDel {
					nonDeleteEvWG.Add(-1)
				}
			}
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ev := range evCh {
					seenPodNames[ev.name] = append(seenPodNames[ev.name], ev)
				}
			}()

			p := NewPodWatcher(cs, "default", "app=fimbat", cb)
			p.Logger = &tbWrap{tb: t}
			runErr := make(chan error)
			go func() {
				err := p.Run(ctx)
				if err != nil {
					t.Errorf("failed to pull initial set of pods: %s", err)
				}
				runErr <- err
			}()

			// Wait for the watcher to start delivering the
			// initial-state list before we start making changes.
			<-firstEventSeen

			eventsSent := make(chan struct{}, 1)
			go func() {
				for _, pi := range tbl.newPods {
					pod := genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
					watcher.Add(pod)
				}
				eventsSent <- struct{}{}
			}()
			select {
			case <-eventsSent:
			case err := <-runErr:
				t.Fatalf("Run exited prematurely: %s", err)
			}

			go func() {
				for _, pi := range tbl.changePods {
					pod := genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
					watcher.Modify(pod)
				}
				eventsSent <- struct{}{}
			}()
			select {
			case <-eventsSent:
			case err := <-runErr:
				t.Fatalf("Run exited prematurely: %s", err)
			}

			// Stop and reset the watcher to ensure that the client
			// reconnects appropriately (picking up the new
			// channel).
			watcher.Stop()
			// wait for the preceding events to make it through the
			// pipeline before calling Reset, which clears the
			// channel's buffer (and allocates a new one)
			nonDeleteEvWG.Wait()
			watcher.Reset()
			for _, dp := range tbl.deletePods {
				watcher.Delete(initPodMap[dp])
			}

			// wait for the expected events
			go func() {
				evWG.Wait()
				eventsSent <- struct{}{}
			}()
			// Set a timeout of 5s so when we lose events we get
			// something useful, rather than a stacktrace saying
			// that we're waiting for *something*
			select {
			case <-time.After(time.Second * 5):
				t.Errorf("timeout after 5s (missing events)")
			case <-eventsSent:
			}
			cancel()
			<-runErr
			close(evCh)
			wg.Wait()

			finishedPhase := func(phase k8score.PodPhase) bool {
				switch phase {
				case k8score.PodRunning, k8score.PodPending:
					return false
				case k8score.PodSucceeded, k8score.PodFailed:
					return true
				default:
					return true
				}
			}

			verifyPI := func(pi podInfo, init bool) {
				pes, ok := seenPodNames[pi.name]
				if !ok && pi.ip != "" && pi.phase == k8score.PodRunning {
					t.Errorf("pod %q has IP and is running but was not present in initial set", pi.name)
					return
				}
				// The initial list should skip anything that's already finished.
				if !ok && finishedPhase(pi.phase) {
					return
				}
				if !ok {
					t.Errorf("missing value for pod %s", pi.name)
					return
				}
				pe := pes[0]
				if pe.IP.String() != pi.ip {
					t.Errorf("mismatched ip for pod %q: in: %s, out: %s", pi.name, pi.ip, pe.IP)
				}
				if !pe.isCreate {
					t.Errorf("isCreate false on initial-creation event for pod: %s", pi.name)
				}

				if pi.ip != pe.pod.Status.PodIP {
					t.Errorf("ip %q not equal to %q", pi.ip, pe.pod.Status.PodIP)
				}
				if pi.name != pe.pod.Name {
					t.Errorf("name %q does not match %q", pi.name, pe.pod.Name)
				}

			}
			verifyChangePI := func(pi podInfo) {
				pes, ok := seenPodNames[pi.name]
				if ok && pi.phase != k8score.PodRunning {
					t.Errorf("pod %q present, but not running (status %s)", pi.name, pi.phase)
					return
				} else if pi.phase != k8score.PodRunning {
					return
				}
				if !ok && pi.ip != "" && pi.phase == k8score.PodRunning {
					t.Errorf("pod %q has IP and is running but was not present in initial set", pi.name)
					return
				}
				if !ok {
					t.Errorf("missing value for pod %s", pi.name)
					return
				}
				pe := pes[1]
				if pe.IP.String() != pi.ip {
					t.Errorf("mismatched ip for pod %q: in: %s, out: %s", pi.name, pi.ip, pe.IP)
				}
				if !pe.isMod {
					t.Errorf("isMod false on modification event for pod: %s", pi.name)
				}

				if pi.ip != pe.pod.Status.PodIP {
					t.Errorf("ip %q not equal to %q", pi.ip, pe.pod.Status.PodIP)
				}
				if pi.name != pe.pod.Name {
					t.Errorf("name %q does not match %q", pi.name, pe.pod.Name)
				}
			}
			for _, pi := range tbl.pods {
				verifyPI(pi, true)
			}
			for _, pi := range tbl.newPods {
				verifyPI(pi, false)
			}
			for _, pi := range tbl.changePods {
				verifyChangePI(pi)
			}
			for _, delPodName := range tbl.deletePods {
				pe, ok := seenPodNames[delPodName]
				if !ok {
					t.Errorf("missing delete event for pod %s", delPodName)
					continue
				}
				if len(pe) < 2 {
					t.Errorf("missing event for pod %s: have %+v", delPodName, pe)
					continue
				}
				peInst := pe[len(pe)-1]
				if peInst.isCreate {
					t.Errorf("last event for pod %s is a create: %[2]T %+[2]v", delPodName, pe)
				}
				if peInst.isMod {
					t.Errorf("last event for pod %s is a mod: %[2]T %+[2]v %+[3]v", delPodName, pe, peInst)
				}
			}

		})
	}
}

type fakeCoreK8sClient struct {
	*fake.Clientset
	core v1.CoreV1Interface
}

func (f *fakeCoreK8sClient) CoreV1() v1.CoreV1Interface {
	return f.core
}

type fakePodGetterCore struct {
	v1.CoreV1Interface
	podImpl v1.PodInterface
}

func (f *fakePodGetterCore) Pods(namespace string) v1.PodInterface {
	return f.podImpl
}

type listRet struct {
	podList *k8score.PodList
	err     error
}

type goneErr struct{}

func (goneErr) Error() string {
	return "gone fimbat"
}

func (goneErr) Status() k8smeta.Status {
	return k8smeta.Status{Reason: k8smeta.StatusReasonGone}
}

type dummyPods struct {
	v1.PodInterface
	// l protects the two slices below so we can pop them off as they're
	// returned.
	l         sync.Mutex
	watchRets []struct {
		w   watch.Interface
		err error
	}
	listRets []listRet
}

func (d *dummyPods) List(opts k8smeta.ListOptions) (*k8score.PodList, error) {
	d.l.Lock()
	defer d.l.Unlock()
	// If the list is empty, just fall back to the fake we're wrapping
	if len(d.listRets) == 0 {
		return d.PodInterface.List(opts)
	}
	front := d.listRets[0]
	d.listRets = d.listRets[1:]

	return front.podList, front.err
}

func (d *dummyPods) Watch(opts k8smeta.ListOptions) (watch.Interface, error) {
	d.l.Lock()
	defer d.l.Unlock()
	// If the list is empty, just fall back to the fake we're wrapping
	if len(d.watchRets) == 0 {
		return d.PodInterface.Watch(opts)
	}
	front := d.watchRets[0]
	d.watchRets = d.watchRets[1:]

	return front.w, front.err
}

func TestPodWatcherErrorRecovery(t *testing.T) {
	type podInfo struct {
		name, ip string
		labels   map[string]string
		ready    bool
		phase    k8score.PodPhase
	}
	type listRetPI struct {
		pi   []podInfo
		err  error
		vers string
	}
	type watchEvent struct {
		pi        podInfo
		vers      string
		eventType watch.EventType
	}
	type watchRet struct {
		watch []watchEvent
		err   error
	}
	for _, itbl := range []struct {
		name           string
		listRets       []listRetPI
		watchRets      []watchRet
		expectedEvents []PodEvent
	}{
		{
			name: "one_ready",
			listRets: []listRetPI{{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}}}},
			expectedEvents: []PodEvent{&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
				Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)}},
			watchRets: []watchRet{{watch: []watchEvent{}}},
		},
		{
			name: "two_ready",
			listRets: []listRetPI{{pi: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
			}}},
			watchRets: []watchRet{{watch: []watchEvent{}}},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
				&CreatePod{name: "foobar2", IP: &net.IPAddr{IP: net.IPv4(10, 42, 43, 41)},
					Def: genPod("foobar2", "10.42.43.41", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
			},
		},
		{
			name: "two_not_ready",
			listRets: []listRetPI{{pi: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			}}},
			watchRets: []watchRet{{watch: []watchEvent{}}},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, false, k8score.PodRunning)},
				&CreatePod{name: "foobar2", IP: &net.IPAddr{IP: net.IPv4(10, 42, 43, 41)},
					Def: genPod("foobar2", "10.42.43.41", map[string]string{"app": "fimbat"}, false, k8score.PodRunning)},
			},
		},
		{
			name: "two_not_ready_two_not_running",
			listRets: []listRetPI{{pi: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			}}},
			watchRets: []watchRet{{watch: []watchEvent{}}},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, false, k8score.PodRunning)},
				&CreatePod{name: "foobar2", IP: &net.IPAddr{IP: net.IPv4(10, 42, 43, 41)},
					Def: genPod("foobar2", "10.42.43.41", map[string]string{"app": "fimbat"}, false, k8score.PodRunning)},
			},
		},
		{
			name: "one_ready_then_dies",
			listRets: []listRetPI{{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}}}},
			watchRets: []watchRet{{
				watch: []watchEvent{
					{pi: podInfo{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
						ready: false, phase: k8score.PodFailed}, eventType: watch.Deleted}}}},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
				&DeletePod{name: "foobar"},
			},
		},
		{
			name: "one_ready_then_dies_one_reconnect",
			listRets: []listRetPI{{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}}}},
			watchRets: []watchRet{
				{watch: []watchEvent{}},
				{err: goneErr{}},
				{watch: []watchEvent{}},
			},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
				&DeletePod{name: "foobar"},
			},
		},
		{
			name: "one_ready_then_dies_one_reconnect_dedup_delete",
			listRets: []listRetPI{{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}}}},
			watchRets: []watchRet{
				{watch: []watchEvent{}},
				{err: goneErr{}},
				{watch: []watchEvent{
					{pi: podInfo{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
						ready: false, phase: k8score.PodFailed}},
				}},
			},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
				&DeletePod{name: "foobar"},
			},
		},
		{
			name: "one_ready_and_survives_one_reconnect",
			listRets: []listRetPI{
				{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}}},
				{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}}},
			},
			watchRets: []watchRet{
				{watch: []watchEvent{}},
				{err: goneErr{}},
				{watch: []watchEvent{}},
			},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
			},
		},
		{
			name: "one_ready_and_survives_one_reconnect_one_new_then_ready",
			listRets: []listRetPI{
				{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}}},
				{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}}},
				{pi: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}, {name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning}}},
			},
			watchRets: []watchRet{
				{watch: []watchEvent{}},
				{err: goneErr{}},
				{watch: []watchEvent{
					{pi: podInfo{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
						ready: false, phase: k8score.PodRunning}, eventType: watch.Added},
				}},
				{err: goneErr{}},
				{watch: []watchEvent{}},
			},
			expectedEvents: []PodEvent{
				&CreatePod{name: "foobar", IP: &net.IPAddr{IP: net.IPv4(10, 42, 42, 42)},
					Def: genPod("foobar", "10.42.42.42", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
				&CreatePod{name: "foobar2", IP: &net.IPAddr{IP: net.IPv4(10, 42, 43, 41)},
					Def: genPod("foobar2", "10.42.43.41", map[string]string{"app": "fimbat"}, false, k8score.PodRunning)},
				&ModPod{name: "foobar2", IP: &net.IPAddr{IP: net.IPv4(10, 42, 43, 41)},
					Def: genPod("foobar2", "10.42.43.41", map[string]string{"app": "fimbat"}, true, k8score.PodRunning)},
			},
		},
	} {
		tbl := itbl
		t.Run(tbl.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listRets := make([]listRet, len(tbl.listRets))
			for i, fim := range tbl.listRets {
				// if pi is nil, populate that entry with a nil
				// PodList and the error
				if fim.pi == nil {
					listRets[i] = listRet{podList: nil, err: fim.err}
					continue
				}
				retPods := make([]k8score.Pod, len(fim.pi))
				for z, pi := range fim.pi {
					retPods[z] = *genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
				}
				rList := k8score.PodList{
					ListMeta: k8smeta.ListMeta{
						ResourceVersion:    fim.vers,
						RemainingItemCount: nil,
					},
					Items: retPods,
				}
				listRets[i] = listRet{podList: &rList, err: fim.err}
			}

			wrs := make([]struct {
				w   watch.Interface
				err error
			}, len(tbl.watchRets))
			for z, wr := range tbl.watchRets {
				if wr.watch == nil {
					wrs[z] = struct {
						w   watch.Interface
						err error
					}{w: nil, err: wr.err}
				}
				eventSlice := wr.watch[:]
				// construct our fake watcher such that it has
				// an internal channel that is large enough for
				// all events
				fw := watch.NewFakeWithChanSize(len(eventSlice), false)
				for _, ev := range eventSlice {
					p := genPod(ev.pi.name, ev.pi.ip,
						ev.pi.labels, ev.pi.ready,
						ev.pi.phase)
					p.ResourceVersion = ev.vers
					fw.Action(ev.eventType, p)
				}
				// Stop the watcher so it closes the internal
				// channel
				fw.Stop()
				wrs[z] = struct {
					w   watch.Interface
					err error
				}{w: fw, err: wr.err}
			}

			cs := fake.NewSimpleClientset()
			cv1 := cs.CoreV1()
			dpfake := cv1.Pods(defaultNamespace)
			dp := dummyPods{PodInterface: dpfake,
				watchRets: wrs,
				listRets:  listRets,
			}
			fpg := fakePodGetterCore{CoreV1Interface: cv1, podImpl: &dp}
			fakeCS := fakeCoreK8sClient{core: &fpg, Clientset: cs}
			// append to a single slice without a lock so the
			// race-detector will complain if we start executing
			// a particular callback from multiple goroutines
			// without synchronization.
			events := []PodEvent{}
			cb := func(ctx context.Context, ev PodEvent) {
				events = append(events, ev)
				if len(events) >= len(tbl.expectedEvents) {
					// if we have the right number of
					// events, we're done.
					cancel()
				}
			}
			p := NewPodWatcher(&fakeCS, defaultNamespace, "app=fimbat", cb)

			runErrCh := make(chan error, 1)
			go func() {
				runErrCh <- p.Run(ctx)
			}()

			if runErr := <-runErrCh; runErr != nil {
				t.Errorf("run failed: %s", runErr)
			}

			if len(events) != len(tbl.expectedEvents) {
				t.Fatalf("mismatched event-slice lengths: got %d; expected %d", len(events), len(tbl.expectedEvents))
			}

			for z, ev := range events {
				if !reflect.DeepEqual(ev, tbl.expectedEvents[z]) {
					t.Errorf("mismatched event %d; expected %+v; got %+v", z, tbl.expectedEvents[z], ev)
				}
			}
		})
	}
}
