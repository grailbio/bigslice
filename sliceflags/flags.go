package sliceflags

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/bigslice/exec"
)

var (
	mu        sync.Mutex
	providers = map[string]Provider{}
)

// Provider represents an instance provider that can be configured by setting
// some set of options via Set.
type Provider interface {
	// Name returns the name of a provider instance.
	Name() string
	// Set sets one or more options for the instances to be provided. The
	// options may be specified as key=val.
	Set(string) error
	// ExecOption returns the appropriate exec.Option to request an
	// instance as configured by the currently set options.
	ExecOption() exec.Option

	// DefaultParallelism returns the default degree of parellelism to use
	// for this provider.
	DefaultParallelism() int
}

// RegisterSystemProvider registers a 'system' provider, ie. any
// service that can provide compute systems/instances to bigslice.
func RegisterSystemProvider(name string, provider Provider) {
	mu.Lock()
	defer mu.Unlock()
	if _, present := providers[name]; present {
		log.Panicf("system %s is already registered", name)
	}
	providers[name] = provider
}

// Internal represents an in-process bigslice execution instance.
type Internal struct{}

// Name implements Provider.Name.
func (i *Internal) Name() string {
	return "internal"
}

// Set implements Provider.Set.
func (i *Internal) Set(_ string) error {
	return fmt.Errorf("the internal instance provider does not support any configuration")
}

// ExecOption implements Provider.ExecOption.
func (i *Internal) ExecOption() exec.Option {
	return exec.Local
}

// DefaultParallelism implements Provider.DefaultParallelism.
func (i *Internal) DefaultParallelism() int {
	return runtime.GOMAXPROCS(0)
}

// Local represents a local machine (separate process) bigmachine execution instance.
type Local struct{}

// Name implements Provider.Name.
func (l *Local) Name() string {
	return "local"
}

// Set implements Provider.Set.
func (l *Local) Set(_ string) error {
	return fmt.Errorf("the internal instance provider does not support any configuration")
}

// ExecOption implements Provider.ExecOption.
func (l *Local) ExecOption() exec.Option {
	return exec.Bigmachine(bigmachine.Local)
}

// DefaultParallelism implements Provider.DefaultParallelism.
func (l *Local) DefaultParallelism() int {
	return runtime.GOMAXPROCS(0)
}

// EC2 represents an AWS EC2 bigmachine instance.
type EC2 struct {
	Options map[string]interface{}
}

// Name implements Provider.Name.
func (ec2 *EC2) Name() string {
	return "EC2"
}

// Set implements Provider.Set.
func (ec2 *EC2) Set(v string) error {
	if ec2.Options == nil {
		ec2.Options = make(map[string]interface{}, 5)
	}
	parts := strings.Split(v, "=")
	if len(parts) != 2 {
		return fmt.Errorf("not in key=val format %q", v)
	}
	key, val := parts[0], parts[1]
	switch key {
	case "dataspace", "rootsize":
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("not an int: %v", val)
		}
		ec2.Options[key] = uint(i)
	case "instance":
		ec2.Options[key] = val
	default:
		return fmt.Errorf("unsupported option: %v", key)
	}
	return nil
}

// DefaultParallelism implements Provider.DefaultParallelism.
func (ec2 *EC2) DefaultParallelism() int {
	return runtime.GOMAXPROCS(0)
}

// ExecOption implements Provider.ExecOption.
func (ec2 *EC2) ExecOption() exec.Option {
	if ec2.Options == nil {
		return exec.Bigmachine(&ec2system.System{})
	}
	instance := &ec2system.System{}
	for key, val := range ec2.Options {
		switch key {
		case "instance":
			instance.InstanceType = val.(string)
		case "dataspace":
			instance.Dataspace = val.(uint)
		case "rootsize":
			instance.Diskspace = val.(uint)
		}
	}
	return exec.Bigmachine(instance)
}

func init() {
	RegisterSystemProvider("local", &Local{})
	RegisterSystemProvider("internal", &Internal{})
	RegisterSystemProvider("ec2", &EC2{})
}

// SystemHelpShort is a short explanation of the allowed SystemFlags values.
const SystemHelpShort = `
a bigmachine system is specified as follows: {local,internal,ec2:[key=val,]
`

// SystemHelpLong is a completion explanation of the allowed SystemFlags values.
const SystemHelpLong = `
A bigmachine system is specified as follows:

<system-type>:<options> where options is [key=value,]+

The currently supported instance types and their options are as follows:

internal: in-process execution, the default.
local: same machine, separate process execution.
ec2: AWS EC2 execution. The supported options are:
    instance=<AWS instance type> - the AWS instance type, e.g. m4.xlarge
	dataspace=<number> - size of the data volume in GiB, typically /mnt/data.
	rootsize=<number> - size of the root volume in GiB.
`

// SystemFlag represents a flag that can be used to specify a bigmachine
// instance.
type SystemFlag struct {
	Provider  Provider
	Options   []string
	Specified bool
}

// String implements flag.Value.String
func (sys *SystemFlag) String() string {
	if sys.Provider == nil {
		return ""
	}
	if sys.Options == nil {
		return sys.Provider.Name()
	}
	opts := make([]string, 0, 5)
	for _, v := range sys.Options {
		opts = append(opts, v)
	}
	return fmt.Sprintf("%v:%v", sys.Provider.Name(), strings.Join(opts, ","))
}

// Set implements flag.Value.Set
func (sys *SystemFlag) Set(v string) error {
	parts := strings.Split(v, ":")
	name := ""
	switch len(parts) {
	case 2:
		sys.Options = strings.Split(parts[1], ",")
		fallthrough
	case 1:
		name = parts[0]
	default:
		return fmt.Errorf("invalid format expected <type>:<options>, got %v", v)
	}
	mu.Lock()
	defer mu.Unlock()
	provider, ok := providers[name]
	if !ok {
		return fmt.Errorf("unspported system type: %v", sys.Provider)
	}
	for _, opt := range sys.Options {
		if err := provider.Set(opt); err != nil {
			return err
		}
	}
	sys.Provider = provider
	sys.Specified = true
	return nil
}

// Get implements flag.Value.Get
func (sys *SystemFlag) Get() interface{} {
	return sys.String()
}

// Flags represents all of the flags that can be used to configure
// a bigslice command.
type Flags struct {
	System        SystemFlag
	HTTPAddress   cmdutil.NetworkAddressFlag
	ConsoleStatus bool
	Parallelism   int
	LoadFactor    float64
}

// RegisterFlags registers the bigslice command line flags with the supplied
// flag set. The flag names will be prefixed with the supplied prefix.
func RegisterFlags(fs *flag.FlagSet, bf *Flags, prefix string) {
	fs.Var(&bf.System, prefix+"system", SystemHelpShort)
	bf.System.Set("local")
	bf.System.Specified = false
	fs.Var(&bf.HTTPAddress, prefix+"http", "address of http status server")
	fs.BoolVar(&bf.ConsoleStatus, prefix+"console-status", false, "print status to stdout")
	fs.IntVar(&bf.Parallelism, prefix+"parallelism", 0, "maximum degree of parallelism to use in terms of CPU cores, 0 requests an appropriate default for the system")
	fs.Float64Var(&bf.LoadFactor, prefix+"load-factor", exec.DefaultMaxLoad, "maximum machine load specified as a percentage")
}

// ExecOptions parses the flag values and returns a slice of exec.Options
// that represent the actions specified by those flags.
func (bf Flags) ExecOptions() ([]exec.Option, error) {
	options := []exec.Option{exec.Status(&status.Status{})}
	options = append(options, bf.System.Provider.ExecOption())
	if bf.Parallelism > 0 {
		options = append(options, exec.Parallelism(bf.Parallelism))
	} else {
		options = append(options, exec.Parallelism(bf.System.Provider.DefaultParallelism()))
	}
	options = append(options, exec.MaxLoad(bf.LoadFactor))
	return options, nil
}
