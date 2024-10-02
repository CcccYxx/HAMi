package plugin

import (
	"context"
	"net"
	"os"
	"path"
	"strconv"
	"time"
	"os/exec"
	"strings"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"github.com/Project-HAMi/HAMi/pkg/device-plugin/nvidiadevice/nvinternal/rm"
)

// DetectTotalGPUCores detects the total number of GPU cores using nvidia-smi
func DetectTotalGPUCores() int {
	// Run the nvidia-smi command to get the GPU count
	cmd := exec.Command("nvidia-smi", "--query-gpu=name", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		klog.Fatalf("Error running nvidia-smi: %v", err)
	}

	// Count the number of lines in the output, which represents the number of GPUs
	gpuCount := len(strings.Split(strings.TrimSpace(string(output)), "\n"))

	// Multiply the physical GPU count by 100
	return gpuCount * 100
}

type GPUCoresDevicePlugin struct {
	resourceName string
	server        *grpc.Server
	socketPath    string
	numDevices    int // Number of GPU cores devices to advertise
}

// NewGPUCoresDevicePlugin creates and initializes the device plugin
func NewGPUCoresDevicePlugin() *GPUCoresDevicePlugin {
	// Detect total GPU cores and initialize plugin
	numDevices := DetectTotalGPUCores()
	return &GPUCoresDevicePlugin{
		resourceName:  "nvidia.com/gpucores",
		server: 		grpc.NewServer([]grpc.ServerOption{}...),
		numDevices:     numDevices,
		socketPath:     pluginapi.DevicePluginPath + "gpucores.sock",
	}
}

// Not Implemented
func (p *GPUCoresDevicePlugin) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// Not Implemented
func (p *GPUCoresDevicePlugin) Devices() rm.Devices {
	return rm.Devices{"dummy_gpucores": nil}
}

// ListAndWatch advertises the GPU core "devices" to Kubernetes
func (p *GPUCoresDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	var devices []*pluginapi.Device

	// Create multiple "devices" based on total GPU cores
	for i := 0; i < p.numDevices; i++ {
		devices = append(devices, &pluginapi.Device{
			ID:     "gpucores" + strconv.Itoa(i), // Device ID
			Health: pluginapi.Healthy,          // Device health
		})
	}

	// Send the device list to the Kubelet in a loop
	for {
		s.Send(&pluginapi.ListAndWatchResponse{Devices: devices})
		time.Sleep(5 * time.Second)
	}
	return nil
}

// Allocate handles GPU cores allocation
func (p *GPUCoresDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	response := &pluginapi.AllocateResponse{}

	// For each container request, allocate dummy GPU cores devices
	for _, req := range reqs.ContainerRequests {
		var containerResp pluginapi.ContainerAllocateResponse
		var gpucores int = 0 // counter for gpucores
		for _, id := range req.DevicesIDs {
			klog.V(5).Info("Allocated gpucore %s", id)
			gpucores++
		}
		klog.Infof("Allocated %d gpu cores", gpucores)
		response.ContainerResponses = append(response.ContainerResponses, &containerResp)
	}

	return response, nil
}

// Register registers the device plugin with the Kubelet
func (p *GPUCoresDevicePlugin) Register() error {
    conn, err := grpc.Dial(
        "unix://"+path.Join(pluginapi.DevicePluginPath, "kubelet.sock"),  // Correctly use Unix domain socket
        grpc.WithInsecure(), // No need for TLS on a local connection
        grpc.WithBlock(),    // Block until connection is established
        grpc.WithTimeout(10*time.Second),  // Set a timeout for the connection
    )
    if err != nil {
        klog.Fatalf("Failed to connect to Kubelet: %v", err)
        return err
    }
    defer conn.Close()

    client := pluginapi.NewRegistrationClient(conn)
    req := &pluginapi.RegisterRequest{
        Version:      pluginapi.Version,
        Endpoint:     path.Base(p.socketPath), // Name of the plugin's Unix socket file
        ResourceName: p.resourceName,            // e.g., "nvidia.com/gpumem"
    }

    _, err = client.Register(context.Background(), req)
    if err != nil {
        klog.Fatalf("Failed to register device plugin: %v", err)
        return err
    }

    klog.Infof("Device plugin registered with Kubelet: %s", p.resourceName)
    return nil
}

// Not implemented
func (p *GPUCoresDevicePlugin) GetPreferredAllocation(
    ctx context.Context, 
    req *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
    return &pluginapi.PreferredAllocationResponse{}, nil
}

// Not implemented
func (p *GPUCoresDevicePlugin) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// Credit: https://github.com/Project-HAMi/HAMi/blob/16824547766b28ab0dd8e77e9a4a3b3233959d59/pkg/device-plugin/nvidiadevice/nvinternal/plugin/server.go#L124
func (p *GPUCoresDevicePlugin) Serve() error {
	// Clean up existing socket if any
	os.Remove(p.socketPath)
	sock, err := net.Listen("unix", p.socketPath)
	if err != nil {
		return err
	}
	pluginapi.RegisterDevicePluginServer(p.server, p)
	
	go func() {
		lastCrashTime := time.Now()
		restartCount := 0
		for {
			klog.Infof("Starting GRPC server for '%s'", p.resourceName)
			err := p.server.Serve(sock)
			if err == nil {
				break
			}

			klog.Infof("GRPC server for '%s' crashed with error: %v", p.resourceName, err)

			// restart if it has not been too often
			// i.e. if server has crashed more than 5 times and it didn't last more than one hour each time
			if restartCount > 5 {
				// quit
				klog.Fatalf("GRPC server for '%s' has repeatedly crashed recently. Quitting", p.resourceName)
			}
			timeSinceLastCrash := time.Since(lastCrashTime).Seconds()
			lastCrashTime = time.Now()
			if timeSinceLastCrash > 3600 {
				// it has been one hour since the last crash.. reset the count
				// to reflect on the frequency
				restartCount = 1
			} else {
				restartCount++
			}
		}
	}()

	// Wait for server to start by launching a blocking connexion
	conn, err := p.dial(p.socketPath, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

func (p *GPUCoresDevicePlugin) Start() error {
	err := p.Serve()
	if err != nil {
		klog.Infof("Could not start device plugin for '%s': %s", p.resourceName, err)
		p.CleanUp()
		return err
	}
	klog.Infof("Starting to serve '%s' on %s", p.resourceName, p.socketPath)

	err = p.Register()
	if err != nil {
		klog.Infof("Could not register device plugin: %s", err)
		p.Stop()
		return err
	}

	return nil
}

// dial establishes the gRPC communication with the registered device plugin.
func (p *GPUCoresDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (p *GPUCoresDevicePlugin) CleanUp() {
	p.server = nil
}


// Stop stops the gRPC server.
func (p *GPUCoresDevicePlugin) Stop() error {
	if p == nil || p.server == nil {
		return nil
	}
	klog.Infof("Stopping to serve '%s' on %s", p.resourceName, p.socketPath)
	p.server.Stop()
	if err := os.Remove(p.socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	p.CleanUp()
	return nil
}
