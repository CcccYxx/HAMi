package plugin

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	kubeletdevicepluginv1beta1 "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"github.com/Project-HAMi/HAMi/pkg/device-plugin/nvidiadevice/nvinternal/rm"
)

// Function to detect the total GPU memory on the node by querying `nvidia-smi`
func DetectTotalGPUMemory() int {
	// Execute nvidia-smi to get memory information
	cmd := exec.Command("nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits")
	var out bytes.Buffer
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		klog.Fatalf("Failed to run nvidia-smi: %v", err)
	}

	// Parse the output and sum the memory values
	totalMemory := 0
	lines := strings.Split(out.String(), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Convert the memory string (in MiB) to an integer
		memory, err := strconv.Atoi(line)
		if err != nil {
			klog.Errorf("Failed to parse memory value: %v", err)
			continue
		}
		totalMemory += memory
	}

	return totalMemory
}

type GPUMemDevicePlugin struct {
	resourceName  string
	server        *grpc.Server
	totalGPUMemory int
	socketPath    string
	numDevices    int // Number of GPU memory devices to advertise
}

// NewGPUMemDevicePlugin creates and initializes the device plugin
func NewGPUMemDevicePlugin() *GPUMemDevicePlugin {
	// Detect total GPU memory and initialize plugin
	totalGPUMemory := DetectTotalGPUMemory()
	numDevices := totalGPUMemory // Divide total memory into devices
	return &GPUMemDevicePlugin{
		resourceName: 	"nvidia.com/gpumem",
		server: 		grpc.NewServer([]grpc.ServerOption{}...),
		totalGPUMemory: totalGPUMemory,
		numDevices:     numDevices,
		socketPath:     pluginapi.DevicePluginPath + "gpumem.sock",
	}
}

// Not Implemented
func (p *GPUMemDevicePlugin) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// Not Implemented
func (p *GPUMemDevicePlugin) Devices() rm.Devices {
	return rm.Devices{"dummy_gpumem": nil}
}

// ListAndWatch advertises the GPU memory "devices" to Kubernetes
func (p *GPUMemDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	var devices []*pluginapi.Device

	// Create multiple "devices" based on total GPU memory
	for i := 0; i < p.numDevices; i++ {
		devices = append(devices, &pluginapi.Device{
			ID:     "gpumem" + strconv.Itoa(i), // Device ID
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

// Allocate handles GPU memory allocation
func (p *GPUMemDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	response := &pluginapi.AllocateResponse{}

	// For each container request, allocate dummy GPU memory devices
	for _, req := range reqs.ContainerRequests {
		var containerResp pluginapi.ContainerAllocateResponse
		var gpumem_mb int = 0
		// counter for gpumem
		for _, id := range req.DevicesIDs {
			klog.V(5).Info("Allocated gpumem %s", id)
			gpumem_mb++
		}
		klog.Infof("Allocated %d MiB of gpu memory", gpumem_mb)
		response.ContainerResponses = append(response.ContainerResponses, &containerResp)
	}

	return response, nil
}

// Register registers the device plugin with the Kubelet
func (p *GPUMemDevicePlugin) Register() error {
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
func (p *GPUMemDevicePlugin) GetPreferredAllocation(
    ctx context.Context, 
    req *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
    return &pluginapi.PreferredAllocationResponse{}, nil
}

// Not implemented
func (p *GPUMemDevicePlugin) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// Credit: https://github.com/Project-HAMi/HAMi/blob/16824547766b28ab0dd8e77e9a4a3b3233959d59/pkg/device-plugin/nvidiadevice/nvinternal/plugin/server.go#L124
func (p *GPUMemDevicePlugin) Serve() error {
	// Clean up existing socket if any
	os.Remove(p.socketPath)
	sock, err := net.Listen("unix", p.socketPath)
	if err != nil {
		return err
	}
	kubeletdevicepluginv1beta1.RegisterDevicePluginServer(p.server, p)
	
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

func (p *GPUMemDevicePlugin) Start() error {
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
func (plugin *GPUMemDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
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

func (plugin *GPUMemDevicePlugin) CleanUp() {
	plugin.server = nil
}


// Stop stops the gRPC server.
func (p *GPUMemDevicePlugin) Stop() error {
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