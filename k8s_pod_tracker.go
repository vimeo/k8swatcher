package k8swatcher

import (
	"net"
	"reflect"

	k8score "k8s.io/api/core/v1"
)

type podTracker struct {
	lastStatus  map[string]*k8score.Pod
	lastVersion string
}

func (p *podTracker) recordEvent(ev PodEvent, vers string) {

	p.lastVersion = vers

	switch pe := ev.(type) {
	case *CreatePod:
		p.lastStatus[ev.PodName()] = pe.Def
	case *ModPod:
		p.lastStatus[ev.PodName()] = pe.Def
	case *DeletePod:
		// We don't care about the state, we'll just delete any entry
		// that's here for this pod.
		delete(p.lastStatus, ev.PodName())
	}
}

// Looks up the previous record (if any) and synthesizes an event for this pod.
// The event may be nil if no event should be generated.
func (p *podTracker) synthesizeEvent(pod *k8score.Pod) PodEvent {
	podName := pod.ObjectMeta.Name
	podIP := pod.Status.PodIP
	ipaddr := net.ParseIP(podIP)
	oldpod, present := p.lastStatus[podName]

	// update our existing record
	p.lastStatus[podName] = pod

	if pod.ResourceVersion != "" {
		p.lastVersion = pod.ResourceVersion
	}

	if present {
		// if nothing changed, just return nil
		if reflect.DeepEqual(pod.Status, oldpod.Status) &&
			reflect.DeepEqual(pod.ObjectMeta, oldpod.ObjectMeta) {
			return nil
		}
		return &ModPod{
			name: podName,
			IP: &net.IPAddr{
				IP:   ipaddr,
				Zone: "",
			},
			Def: pod,
		}
	}
	return &CreatePod{
		name: podName,
		IP: &net.IPAddr{
			IP:   ipaddr,
			Zone: "",
		},
		Def: pod,
	}
}

// findRemoveDeadPods returns a set of dead pods
func (p *podTracker) findRemoveDeadPods(existingPodNames []string) map[string]struct{} {
	oldPods := make(map[string]struct{}, len(p.lastStatus))
	for podName := range p.lastStatus {
		oldPods[podName] = struct{}{}
	}
	for _, pod := range existingPodNames {
		delete(oldPods, pod)
	}
	// At this point the remaining pods in oldPods are dead
	// now we update the existing state map to remove the dead pods.
	for pn := range oldPods {
		delete(p.lastStatus, pn)
	}
	return oldPods
}
