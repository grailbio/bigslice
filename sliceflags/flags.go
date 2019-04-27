// Package sliceflags provides flag support for use by bigslice command
// line applications.
package sliceflags

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
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
	providers = map[string]Provider{} // protected by mu
	profiles  = map[string]string{}   // protected by mu
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

// RegisterSystemProfile registers a system 'profile' which
// is a named shorthand for a system and any associated options.
// For example an application that registers a profile of:
//   sliceflags.RegisterSystemProfile("my-ec2-app", "ec2:dataspace=500")
// can accept
//   --system=my-ec2-app
// as a synonym for
//   --system=ec2:dataspace=500
func RegisterSystemProfile(name, profile string) {
	mu.Lock()
	defer mu.Unlock()
	if _, present := providers[name]; present {
		log.Panicf("profile %s is already used as a provider name", name)
	}
	if _, present := profiles[name]; present {
		log.Panicf("profile %s is already registered", name)
	}
	profiles[name] = profile
}

// ProvidersAndProfiles returns the supported providers and profiles.
func ProvidersAndProfiles() ([]string, map[string]string) {
	mu.Lock()
	defer mu.Unlock()
	prv := make([]string, 0, len(providers))
	for k := range providers {
		prv = append(prv, k)
	}
	prf := make(map[string]string, len(profiles))
	for k, v := range profiles {
		prf[k] = v
	}
	return prv, prf
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
	case "instance", "profile":
		ec2.Options[key] = val
	case "ondemand":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("not a bool: %v", val)
		}
		ec2.Options[key] = b
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
	instance := &ec2system.System{
		Username: "unknown",
	}
	u, err := user.Current()
	if err == nil {
		instance.Username = u.Username
	} else {
		log.Printf("newec2: get current user: %v", err)
	}
	for key, val := range ec2.Options {
		switch key {
		case "instance":
			instance.InstanceType = val.(string)
		case "dataspace":
			instance.Dataspace = val.(uint)
		case "rootsize":
			instance.Diskspace = val.(uint)
		case "profile":
			instance.InstanceProfile = val.(string)
		case "ondemand":
			instance.OnDemand = val.(bool)
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
func SystemHelpShort(prefix string) string {
	const format = `a bigslice system is specified as follows: {local,internal,ec2:[key=val,],name}, use -%s for more information.`
	return fmt.Sprintf(format, prefix+"system-help")
}

// SystemHelpLong is a completion explanation of the allowed SystemFlags values.
const SystemHelpLong = `A bigslice system is specified as follows:

<system-type>:<options> where options is [key=value,]+

The currently supported instance types and their options are as follows:

internal: in-process execution, the default.
local: same machine, separate process execution.
ec2: AWS EC2 execution. The supported options are:
	instance=<AWS instance type> - the AWS instance type, e.g. m4.xlarge
	dataspace=<number> - size of the data volume in GiB, typically /mnt/data.
	rootsize=<number> - size of the root volume in GiB.
	ondemand - true to use on-demand rather than spot instances
	profile - the aws instance profile to use instead of a default

In addition, an application may register 'profiles' that are shorthand
for the above, eg. "my-app" can be configured as a synonymn for
ec2:m4.xlarge,dataspace=200.
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
	if len(sys.Options) == 0 {
		return sys.Provider.Name()
	}
	return fmt.Sprintf("%v:%v", sys.Provider.Name(), strings.Join(sys.Options, ","))
}

// Set implements flag.Value.Set
func (sys *SystemFlag) Set(v string) error {
	parse := func(s string) (name string, options []string) {
		parts := strings.SplitN(s, ":", 2)
		name = parts[0]
		if len(parts) > 1 {
			options = strings.Split(parts[1], ",")
		}
		return
	}

	name, options := parse(v)
	mu.Lock()
	if profile, ok := profiles[name]; ok {
		var profileOptions []string
		name, profileOptions = parse(profile)
		options = append(profileOptions, options...)
	}
	provider, ok := providers[name]
	mu.Unlock()
	if !ok {
		return fmt.Errorf("unsupported system or profile type: %v", name)
	}
	for _, opt := range options {
		if err := provider.Set(opt); err != nil {
			return err
		}
	}
	sys.Options = options
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
	SystemHelp    bool
	HTTPAddress   cmdutil.NetworkAddressFlag
	ConsoleStatus bool
	Parallelism   int
	LoadFactor    float64
	fs            *flag.FlagSet
}

// Output returns an appropriate io.Writer for printing out help/usage
// messages as per the underlying flag.Flagset.
func (bf *Flags) Output() io.Writer {
	if bf.fs == nil {
		return os.Stderr
	}
	if wr := bf.fs.Output(); wr != nil {
		return wr
	}
	return os.Stderr
}

// RegisterFlags registers the bigslice command line flags with the supplied
// flag set. The flag names will be prefixed with the supplied prefix.
func RegisterFlags(fs *flag.FlagSet, bf *Flags, prefix string) {
	RegisterFlagsWithDefaults(fs, bf, prefix, Defaults{
		System:        "internal",
		HTTPAddress:   ":3333",
		ConsoleStatus: false,
		Parallelism:   0,
		LoadFactor:    exec.DefaultMaxLoad,
	})
}

// ExecOptions parses the flag values and returns a slice of exec.Options
// that represent the actions specified by those flags.
func (bf *Flags) ExecOptions() ([]exec.Option, error) {
	var sliceStatus status.Status
	// Ensure bigmachine's group is displayed first.
	_ = sliceStatus.Group(exec.BigmachineStatusGroup)
	_ = sliceStatus.Groups()

	options := []exec.Option{exec.Status(&sliceStatus)}
	options = append(options, bf.System.Provider.ExecOption())
	if bf.Parallelism > 0 {
		options = append(options, exec.Parallelism(bf.Parallelism))
	} else {
		options = append(options, exec.Parallelism(bf.System.Provider.DefaultParallelism()))
	}
	options = append(options, exec.MaxLoad(bf.LoadFactor))
	return options, nil
}

// Defaults represents default values for the supported flags.
type Defaults struct {
	System        string
	HTTPAddress   string
	ConsoleStatus bool
	Parallelism   int
	LoadFactor    float64
}

// RegisterFlagsWithDefaults registers the bigslice command line flags with
// the supplied flag set and defaults. The flag names will be prefixed with the
// supplied prefix.
func RegisterFlagsWithDefaults(fs *flag.FlagSet, bf *Flags, prefix string, defaults Defaults) {
	fs.Var(&bf.System, prefix+"system", SystemHelpShort(prefix))
	bf.System.Set(defaults.System)
	bf.System.Specified = false
	fs.Var(&bf.HTTPAddress, prefix+"http", "address of http status server")
	bf.HTTPAddress.Set(defaults.HTTPAddress)
	bf.HTTPAddress.Specified = false
	fs.BoolVar(&bf.ConsoleStatus, prefix+"console-status", defaults.ConsoleStatus, "print status to stdout")
	fs.IntVar(&bf.Parallelism, prefix+"parallelism", defaults.Parallelism, "maximum degree of parallelism to use in terms of CPU cores, 0 requests an appropriate default for the system")
	fs.Float64Var(&bf.LoadFactor, prefix+"load-factor", defaults.LoadFactor, "maximum machine load specified as a percentage")
	fs.BoolVar(&bf.SystemHelp, prefix+"system-help", false, "provide help on system providers and profiles")
	bf.fs = fs
}
