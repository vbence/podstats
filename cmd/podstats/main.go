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
	"sync"
	"time"

	cache "github.com/victorspringer/http-cache"
	memory "github.com/victorspringer/http-cache/adapter/memory"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// Reading represents a single sensor reading, a value for a metric at a given time
type Reading struct {
	Value float64
	Time  string
}

// MetricsHolder represents a set of metrics in Prometheus's format
type MetricsHolder struct {
	allLines map[string]map[string]*Reading
	mutex    sync.RWMutex
}

// NewMetrics instantiates an empty MetricsHolder
func NewMetrics() *MetricsHolder {
	m := &MetricsHolder{
		allLines: make(map[string]map[string]*Reading),
		mutex:    sync.RWMutex{},
	}
	return m
}

// PutReadings upgrade reading for the given source (removing any existing readings for the source)
func (m *MetricsHolder) PutReadings(source string, readings map[string]*Reading) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.allLines[source] = readings
}

// CreateHandler return a new `http.HandlerFunc` for a MetricsHolder
func (m *MetricsHolder) CreateHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mutex.RLock()
		defer m.mutex.RUnlock()
		for _, lines := range m.allLines {
			for k, v := range lines {
				w.Write([]byte(k))
				w.Write([]byte(" "))
				w.Write([]byte(fmt.Sprintf("%f", v.Value)))
				w.Write([]byte("\n"))
			}
		}
	}
}

func extractPodSpecs(pods []apiv1.Pod) map[string]*Reading {
	r := make(map[string]*Reading)
	for _, pod := range pods {

		extras := make(map[string]string)
		extras["pod-name"] = pod.Name

		for _, con := range pod.Spec.Containers {
			extras["container-name"] = con.Name

			r["ps_memory_request_bytes"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Requests.Memory().AsDec()),
			}
			r["ps_memory_limit_bytes"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Limits.Memory().AsDec()),
			}
			r["ps_cpu_request_cores"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Requests.Cpu().AsDec()),
			}
			r["ps_cpu_limit_cores"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Limits.Cpu().AsDec()),
			}
			r["ps_storage_request_bytes"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Requests.StorageEphemeral().AsDec()),
			}
			r["ps_storage_limit_bytes"+renderLabels(pod.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(pod.GetCreationTimestamp().Unix()*1000, 10),
				Value: decToFloat64(con.Resources.Limits.StorageEphemeral().AsDec()),
			}
		}
	}

	return r
}

func extractPodMetrics(allMetrics []metricsv1beta1.PodMetrics) map[string]*Reading {
	r := make(map[string]*Reading)
	for _, metrics := range allMetrics {
		extras := make(map[string]string)
		extras["pod-name"] = metrics.Name

		for _, con := range metrics.Containers {
			extras["container-name"] = con.Name

			r["ps_memory_usage_bytes"+renderLabels(metrics.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(metrics.Timestamp.Unix()*1000, 10),
				Value: decToFloat64(con.Usage.Memory().AsDec()),
			}
			r["ps_cpu_usage_cores"+renderLabels(metrics.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(metrics.Timestamp.Unix()*1000, 10),
				Value: decToFloat64(con.Usage.Cpu().AsDec()),
			}
			r["ps_storage_usage_bytes"+renderLabels(metrics.Labels, extras)] = &Reading{
				Time:  strconv.FormatInt(metrics.Timestamp.Unix()*1000, 10),
				Value: decToFloat64(con.Usage.StorageEphemeral().AsDec()),
			}
		}
	}
	return r
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

// Closer follows pod changes through the api
type Closer func()

// ShovelList reads data from a Lister
func ShovelList(lister Lister, name string, holder *MetricsHolder, log *zap.Logger) Closer {
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
				holder.PutReadings(name, list)
			case <-close:
				done = true
			}
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

	podConnector, err := NewPodLister(config, *namespace)
	if err != nil {
		logger.Error("Creating pod connector", zap.Error(err))
		os.Exit(1)
	}
	ShovelList(podConnector, "spec", holder, logger)

	metricsConnector, err := NewMetricsLister(config, *namespace)
	if err != nil {
		logger.Error("Creating metrics connector", zap.Error(err))
		os.Exit(1)
	}
	ShovelList(metricsConnector, "usage", holder, logger)

	memcached, err := memory.NewAdapter(
		memory.AdapterWithAlgorithm(memory.LRU),
		memory.AdapterWithCapacity(100),
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

// Lister reads objects through the get API
type Lister interface {
	List(opts metav1.ListOptions) (map[string]*Reading, error)
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
func (l *MetricsLister) List(opts metav1.ListOptions) (map[string]*Reading, error) {
	list, err := l.clientset.MetricsV1beta1().PodMetricses(l.namespace).List(opts)
	if err != nil {
		return nil, err
	}
	return extractPodMetrics(list.Items), nil
}

// PodLister is a Lister for Metrics
type PodLister struct {
	clientset *kubernetes.Clientset
	namespace string
}

// NewPodLister constructs a PodWatcher
func NewPodLister(config *rest.Config, namespace string) (*PodLister, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &PodLister{
		clientset: clientset,
		namespace: namespace,
	}, nil
}

// List returns the result of a listing API call
func (l *PodLister) List(opts metav1.ListOptions) (map[string]*Reading, error) {
	list, err := l.clientset.CoreV1().Pods(l.namespace).List(opts)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(list.Items))
	for k, v := range list.Items {
		result[k] = v
	}
	return extractPodSpecs(list.Items), nil
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
