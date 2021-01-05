package k8swatcher

import (
	"net"
	"reflect"
	"testing"

	k8score "k8s.io/api/core/v1"
)

func TestPodTrackerCreateDelete(t *testing.T) {
	type podInfo struct {
		name, ip string
		labels   map[string]string
		ready    bool
		phase    k8score.PodPhase
	}
	for _, itbl := range []struct {
		name         string
		pods         []podInfo
		surviving    []string
		expectedDead map[string]struct{}
	}{
		{
			name: "one_ready",
			pods: []podInfo{{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
				ready: true, phase: k8score.PodRunning}},
			surviving:    []string{"foobar"},
			expectedDead: map[string]struct{}{},
		},
		{
			name: "two_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: true, phase: k8score.PodRunning},
			},
			surviving:    []string{"foobar"},
			expectedDead: map[string]struct{}{"foobar2": {}},
		},
		{
			name: "two_not_ready",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			surviving:    []string{"foobar"},
			expectedDead: map[string]struct{}{"foobar2": {}},
		},
		{
			name: "two_not_ready_two_not_running",
			pods: []podInfo{
				{name: "foobar", ip: "10.42.42.42", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
				{name: "foobar2", ip: "10.42.43.41", labels: map[string]string{"app": "fimbat"},
					ready: false, phase: k8score.PodRunning},
			},
			surviving:    []string{"foobar"},
			expectedDead: map[string]struct{}{"foobar2": {}},
		},
	} {
		tbl := itbl
		t.Run(tbl.name, func(t *testing.T) {
			initPods := make([]*k8score.Pod, len(tbl.pods))
			tracker := podTracker{
				lastStatus: map[string]*k8score.Pod{},
			}
			rv := "fizzle"
			for z, pi := range tbl.pods {
				initPods[z] = genPod(pi.name, pi.ip, pi.labels, pi.ready, pi.phase)
				ip := net.ParseIP(pi.ip)
				ce := CreatePod{
					name: pi.name,
					rv:   ResourceVersion(rv),
					IP:   &net.IPAddr{IP: ip},
					Def:  initPods[z],
				}
				tracker.recordEvent(&ce)
			}

			deadPods := tracker.findRemoveDeadPods(tbl.surviving)
			if !reflect.DeepEqual(deadPods, tbl.expectedDead) {
				t.Errorf("unexpected mismatch liveness mismatch: got %v; expected %v",
					deadPods, tbl.expectedDead)
			}
		})
	}
}
