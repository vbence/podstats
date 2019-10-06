package main

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	cache "github.com/victorspringer/http-cache"
	memory "github.com/victorspringer/http-cache/adapter/memory"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// ReadingType distinguishes the kinds of metrics
type ReadingType int

const (
	// Counter represents a monotonous series where new values are added
	Counter ReadingType = iota + 1

	// Instant represents an instantenious value
	Instant
)

// Reading represents a single sensor reading, a value for a metric at a given time
type Reading struct {
	Key   string
	Value float64
	Time  string
	Type  ReadingType
}

// Accept returns a metric updated with the new reading
func (r *Reading) Accept(new *Reading) *Reading {
	result := *r
	if new.Type == Counter {
		result.Value += new.Value
		result.Time = new.Time
	} else if new.Type == Instant {
		result.Value = new.Value
		result.Time = new.Time
	}
	return &result
}

// MetricsHolder represents a set of metrics in Prometheus's format
type MetricsHolder struct {
	lines   map[string]*Reading
	channel chan interface{}
}

// NewMetrics instantiates an empty MetricsHolder
func NewMetrics() *MetricsHolder {
	m := &MetricsHolder{
		lines:   make(map[string]*Reading),
		channel: make(chan interface{}),
	}
	go func() {
		for {
			w, ok := <-m.channel
			reading := w.(*Reading)
			if !ok {
				break
			}
			if val, ok := m.lines[reading.Key]; ok {
				m.lines[reading.Key] = val.Accept(reading)
			} else {
				m.lines[reading.Key] = reading
			}
		}
	}()
	return m
}

// Channel returns channel on which this MetricsHolder accepts new readings
func (m *MetricsHolder) Channel() chan<- interface{} {
	return m.channel
}

// CreateHandler return a new `http.HandlerFunc` for a MetricsHolder
func (m *MetricsHolder) CreateHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for k, v := range m.lines {
			w.Write([]byte(k))
			w.Write([]byte(" "))
			w.Write([]byte(fmt.Sprintf("%f", v.Value)))
			w.Write([]byte(" "))
			w.Write([]byte(fmt.Sprintf("%s", v.Time)))
			w.Write([]byte("\n"))
		}
	}
}

// MetricsExtracor creates metrics out of Kubernetes objects
type MetricsExtracor struct {
	channel chan interface{}
	holder  *MetricsHolder
}

// NewMetricsExtracor constructs a MetricsExtracor
func NewMetricsExtracor(holder *MetricsHolder) *MetricsExtracor {
	channel := make(chan interface{})
	m := &MetricsExtracor{
		channel: channel,
		holder:  holder,
	}
	go func() {
		done := false
		for !done {
			select {
			case o := <-channel:
				switch v := o.(type) {
				case apiv1.Pod:
					m.propagatePodSpecs(v)
				case metricsv1beta1.PodMetrics:
					m.propagatePodMetrics(v)
				}
			}
		}
	}()
	return m
}

func (m *MetricsExtracor) propagatePodSpecs(pod apiv1.Pod) {
	for _, con := range pod.Spec.Containers {
		extras := make(map[string]string)
		extras["pod-name"] = pod.Name
		extras["container-name"] = con.Name

		m.holder.Channel() <- &Reading{
			Key:   "PS_MEMORY_REQUEST_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Requests.Memory().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_MEMORY_LIMIT_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Limits.Memory().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_CPU_REQUEST_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Requests.Cpu().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_CPU_LIMIT_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Limits.Cpu().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_STORAGE_REQUEST_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Requests.StorageEphemeral().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_STORAGE_LIMIT_BYTES" + renderLabels(pod.Labels, extras),
			Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Resources.Limits.StorageEphemeral().AsDec()),
		}
	}
}

func (m *MetricsExtracor) propagatePodMetrics(metrics metricsv1beta1.PodMetrics) {
	for _, con := range metrics.Containers {
		extras := make(map[string]string)
		extras["pod-name"] = metrics.Name
		extras["container-name"] = con.Name

		m.holder.Channel() <- &Reading{
			Key:   "PS_MEMORY_USAGE_BYTES" + renderLabels(metrics.Labels, extras),
			Time:  strconv.FormatInt(metrics.Timestamp.Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Usage.Memory().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_CPU_USAGE_CORES" + renderLabels(metrics.Labels, extras),
			Time:  strconv.FormatInt(metrics.Timestamp.Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Usage.Cpu().AsDec()),
		}
		m.holder.Channel() <- &Reading{
			Key:   "PS_STORAGE_USAGE_BYTES" + renderLabels(metrics.Labels, extras),
			Time:  strconv.FormatInt(metrics.Timestamp.Unix(), 10),
			Type:  Instant,
			Value: decToFloat64(con.Usage.StorageEphemeral().AsDec()),
		}
	}
}

func getOrDef(m map[string]float64, key string, def float64) float64 {
	v, ok := m[key]
	if !ok {
		return def
	}
	return v
}

func decToFloat64(dec *inf.Dec) float64 {
	return float64(dec.UnscaledBig().Int64()) /
		math.Pow(10, float64(dec.Scale()))
}

// Channel returns channel on which this MetricsHolder accepts new readings
func (m *MetricsExtracor) Channel() chan<- interface{} {
	return m.channel
}

// Closer follows pod changes through the api
type Closer func()

// ShovelList reads data from a Lister
func ShovelList(lister Lister, sink chan<- interface{}, log *zap.Logger) Closer {
	close := make(chan struct{})
	listOptions := metav1.ListOptions{AllowWatchBookmarks: true}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		done := false
		for !done {
			select {
			case <-ticker.C:
				list, err := lister.List(listOptions)
				if err != nil {
					log.Error("Listing", zap.Error(err))
				}
				for _, v := range list {
					//fmt.Printf("NewListItem: %+v\n", v)
					sink <- v
				}
			case <-close:
				done = true
			}
		}
	}()
	return func() {
		close <- struct{}{}
	}
}

// NewWatcher creates a Watcher object with the given endpoint
func NewWatcher(watcher Watcher, sink chan<- interface{}, log *zap.Logger) Closer {
	close := make(chan struct{})
	//go doWatch(clientset, namespace, sink, close)
	go func() {
		done := false
		bookmark := ""
		for !done {
			log.Info("Watcher connecting")
			listOptions := metav1.ListOptions{Watch: true, AllowWatchBookmarks: true}
			if bookmark != "" {
				listOptions.ResourceVersion = bookmark
			}
			w, err := watcher.Watch(listOptions)
			if err != nil {
				log.Error("Watcher connecting", zap.Error(err))
			} else {
				events := w.ResultChan()
				open := true
			W:
				for !done && open {
					select {
					case e, open := <-events:
						if !open {
							log.Info("Watcher disconnected")
							break W
						}

						if e.Type == watch.Bookmark {
							pod := apiv1.Pod{}
							err := scheme.Scheme.Convert(e.Object, &pod, nil)
							if err != nil {
								log.Error("Converting bookmark", zap.Error(err))
							}
							log.Debug("Bookmark received", zap.String("bookmark", pod.ResourceVersion))
							bookmark = pod.ResourceVersion
						} else {
							// process
							object, err := watcher.Convert(&e)
							if err != nil {
								log.Error("Converting object", zap.Error(err))
							}
							sink <- object
							//fmt.Printf("NewWatcher: %+v\n", object)
						}
					case <-close:
						done = true
						break W
					}
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()
	return func() {
		close <- struct{}{}
	}
}

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	namespace := flag.String("namespace", "default", "namespace to watch")
	debug := flag.Bool("debug", false, "show debug messages")
	flag.Parse()

	var logger *zap.Logger
	if *debug {
		var err error
		logger, err = zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		logger, err = zap.NewProduction()
		if err != nil {
			panic(err)
		}
	}
	defer logger.Sync() // flushes buffer, if any

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		logger.Error("Loading config", zap.Error(err))
		os.Exit(1)
	}

	holder := NewMetrics()
	extracor := NewMetricsExtracor(holder)

	podConnector, err := NewPodWatcher(config, *namespace)
	if err != nil {
		logger.Error("Creating pod connector", zap.Error(err))
		os.Exit(1)
	}
	NewWatcher(podConnector, extracor.Channel(), logger)

	metricsConnector, err := NewMetricsLister(config, *namespace)
	if err != nil {
		logger.Error("Creating metrics connector", zap.Error(err))
		os.Exit(1)
	}
	ShovelList(metricsConnector, extracor.Channel(), logger)

	memcached, err := memory.NewAdapter(
		memory.AdapterWithAlgorithm(memory.LRU),
		memory.AdapterWithCapacity(10000000),
	)
	if err != nil {
		logger.Error("Creating memory-backed cache", zap.Error(err))
		os.Exit(1)
	}

	cacheClient, err := cache.NewClient(
		cache.ClientWithAdapter(memcached),
		cache.ClientWithTTL(10*time.Second),
		cache.ClientWithRefreshKey("opn"),
	)
	if err != nil {
		logger.Error("Creating cache client", zap.Error(err))
		os.Exit(1)
	}

	handler := http.HandlerFunc(holder.CreateHandler())

	http.Handle("/", cacheClient.Middleware(handler))
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		logger.Error("Listening on HTTP endpoint", zap.Error(err))
		os.Exit(1)
	}
}

// Watcher connects to the watch API and converts its result objects
type Watcher interface {
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	Convert(event *watch.Event) (interface{}, error)
}

// PodWatcher is a Watcher for Pods
type PodWatcher struct {
	clientset *kubernetes.Clientset
	namespace string
}

// NewPodWatcher constructs a PodWatcher
func NewPodWatcher(config *rest.Config, namespace string) (*PodWatcher, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &PodWatcher{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

// Watch implementation
func (p *PodWatcher) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return p.clientset.CoreV1().Pods(p.namespace).Watch(opts)
}

// Convert implementation
func (p *PodWatcher) Convert(event *watch.Event) (interface{}, error) {
	pod := apiv1.Pod{}
	err := scheme.Scheme.Convert(event.Object, &pod, nil)
	if err != nil {
		return nil, err
	}
	return pod, nil
}

// Lister reads objects through the get API
type Lister interface {
	List(opts metav1.ListOptions) ([]interface{}, error)
}

// MetricsLister is a Lister for Metrics
type MetricsLister struct {
	clientset *metrics.Clientset
	namespace string
}

// NewMetricsLister constructs a PodWatcher
func NewMetricsLister(config *rest.Config, namespace string) (*MetricsLister, error) {
	clientset, err := metrics.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &MetricsLister{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

// List returns the result of a listing API call
func (l *MetricsLister) List(opts metav1.ListOptions) ([]interface{}, error) {
	list, err := l.clientset.MetricsV1beta1().PodMetricses(l.namespace).List(opts)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(list.Items))
	for k, v := range list.Items {
		result[k] = v
	}
	return result, nil
}

func usage(pod apiv1.Pod, labels map[string]string) []string {
	result := []string{}
	result = append(result, fmt.Sprintf("ASD{%s}=%d\n", labels, 1))

	return result
}

func renderLabels(labels ...map[string]string) string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	first := true
	for _, l := range labels {
		for k, v := range l {
			if !first {
				buffer.WriteString(", ")
			} else {
				first = false
			}
			buffer.WriteString(labelFor(k))
			buffer.WriteString("=\"")
			buffer.WriteString(v)
			buffer.WriteString("\"")
		}
	}
	buffer.WriteString("}")

	return string(buffer.Bytes())
}

var (
	invalidLabels  = regexp.MustCompile("[^a-zA-Z0-9_]")
	leadUnderscore = regexp.MustCompile("^[_]+")
)

func labelFor(name string) string {
	return string(leadUnderscore.ReplaceAll(invalidLabels.ReplaceAll([]byte(name), []byte("_")), []byte("")))
}
