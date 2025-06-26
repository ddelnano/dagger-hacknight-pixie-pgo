// A generated module for PixieModule functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/hello-dagger/internal/dagger"
	"debug/buildinfo"
	"debug/elf"
	"debug/gosym"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/pprof/profile"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"px.dev/pxapi"
	"px.dev/pxapi/types"
)

const (
	goAppBinary    = "application_binary"
	inputFilepath  = "input.pprof"
	outputFilepath = "output.pprof"
	pxlTmpl        = `
import px

stack_traces = px.DataFrame(table='stack_traces.beta', start_time='-5m')
stack_traces.deployment = stack_traces.ctx['deployment']
stack_traces.container = stack_traces.ctx['container']
# Filter out stack traces to just the go service we are trying to deploy
stack_traces = stack_traces[stack_traces.deployment == '%s']
# TODO(ddelnano): Add ability to filter by container name via Dagger function
stack_traces = stack_traces[stack_traces.container == "app"]
stack_traces.asid = px.asid()
sample_period = px.GetProfilerSamplingPeriodMS()
df = stack_traces.merge(sample_period, how='inner', left_on=['asid'], right_on=['asid'])
df = df.groupby(['profiler_sampling_period_ms']).agg(pprof=('stack_trace', 'count', 'profiler_sampling_period_ms', px.pprof))
df.pprof = px.bytes_to_hex(df.pprof)
px.display(df)
`
)

type PprofMuxer struct {
	results     chan string
	resultCount int
}

func NewPprofMuxer(c chan string) *PprofMuxer {
	return &PprofMuxer{
		results:     c,
		resultCount: 0,
	}
}

var _ pxapi.TableMuxer = (*PprofMuxer)(nil)

func (m *PprofMuxer) AcceptTable(ctx context.Context, metadata types.TableMetadata) (pxapi.TableRecordHandler, error) {
	if metadata.IndexOf("pprof") == -1 {
		return m, errors.New("expected pxl results table to contain 'pprof' column")
	}
	return m, nil
}

var _ pxapi.TableRecordHandler = (*PprofMuxer)(nil)

func (m *PprofMuxer) HandleInit(ctx context.Context, metadata types.TableMetadata) error {
	return nil
}

func (m *PprofMuxer) HandleRecord(ctx context.Context, record *types.Record) error {
	m.resultCount++
	if m.resultCount > 1 {
		return fmt.Errorf("expected exactly one row from pprof table, got %d", len(record.Data))
	}
	datum := record.GetDatum("pprof")
	m.results <- datum.String()

	return nil
}

func (m *PprofMuxer) HandleDone(ctx context.Context) error {
	close(m.results)
	return nil
}

func GetFirstHealthyVizier(client *pxapi.Client, ctx context.Context) (string, error) {
	viziers, err := client.ListViziers(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list viziers: %v", err)
	}

	for _, vizier := range viziers {
		if vizier.Status == pxapi.VizierStatusHealthy {
			return vizier.ID, nil
		}
	}

	return "", fmt.Errorf("no healthy viziers found")
}

func (m *PixieModule) GetImageFromDeployment(ctx context.Context, gcloud_config *dagger.Directory, kubeconfig *dagger.File, deploymentName, containerName string) (string, error) {
	gcloud_config.Export(ctx, "gcloud_config")
	kubeconfig.Export(ctx, "config")
	kubectlContainer := dag.Container().
		From("google/cloud-sdk:alpine").
		WithExec([]string{"gcloud", "components", "install", "kubectl", "gke-gcloud-auth-plugin", "--quiet"}).
		WithFile("/root/.kube/config", kubeconfig, dagger.ContainerWithFileOpts{Permissions: 444, Expand: true}).
		WithDirectory("/root/.config/gcloud", gcloud_config, dagger.ContainerWithDirectoryOpts{Expand: true})

	deploymentParts := strings.Split(deploymentName, "/")
	if len(deploymentParts) != 2 {
		return "", fmt.Errorf("deployment name must be in the format 'namespace/deploymentName', got: %s", deploymentName)
	}

	ns, name := deploymentParts[0], deploymentParts[1]

	var cmd []string
	if containerName != "" {
		cmd = []string{"kubectl", "get", "deployment", name, "-n", ns, "-o", fmt.Sprintf("jsonpath={.spec.template.spec.containers[?(@.name=='%s')].image}", containerName)}
	} else {
		cmd = []string{"kubectl", "get", "deployment", name, "-n", ns, "-o", "jsonpath={.spec.template.spec.containers[0].image}"}
	}

	result, err := kubectlContainer.WithExec(cmd).Stdout(ctx)

	if err != nil {
		return "", fmt.Errorf("failed to get deployment image: %v", err)
	}

	image := strings.TrimSpace(result)
	if image == "" {
		if containerName != "" {
			return "", fmt.Errorf("container '%s' not found in deployment %s", containerName, deploymentName)
		}
		return "", fmt.Errorf("no containers found in deployment %s", deploymentName)
	}

	return image, nil
}

func GetImageFromDeployment(ctx context.Context, deploymentName, containerName string) (string, error) {
	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return "", fmt.Errorf("failed to build kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", fmt.Errorf("failed to create Kubernetes clientset: %v", err)
	}

	deploymentParts := strings.Split(deploymentName, "/")
	if len(deploymentParts) != 2 {
		return "", fmt.Errorf("deployment name must be in the format 'namespace/deploymentName', got: %s", deploymentName)
	}

	ns, name := deploymentParts[0], deploymentParts[1]
	deployment, err := clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get deployment %s: %v", deploymentName, err)
	}

	if len(deployment.Spec.Template.Spec.Containers) > 1 {
		if containerName == "" {
			return "", fmt.Errorf("deployment %s has multiple containers, please specify a container name", deploymentName)
		}

		for _, container := range deployment.Spec.Template.Spec.Containers {
			if container.Name == containerName {
				return container.Image, nil
			}
		}
	}

	return deployment.Spec.Template.Spec.Containers[0].Image, nil
}

type PixieModule struct{}

func (m *PixieModule) CollectProfile(
	ctx context.Context,
	// +default="getcosmic.ai:443"
	cloudAddr,
	vizierId,
	deploymentName string,
	apiKey *dagger.Secret,
) (*dagger.File, error) {
	apiKeyPlaintext, err := apiKey.Plaintext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get plaintext API key: %v", err)
	}

	client, err := pxapi.NewClient(ctx, pxapi.WithCloudAddr(cloudAddr), pxapi.WithAPIKey(apiKeyPlaintext))
	if err != nil {
		return nil, fmt.Errorf("failed to create Pixie API client: %v", err)
	}

	if vizierId == "" {
		vizierId, err = GetFirstHealthyVizier(client, ctx)

		if err != nil {
			return nil, fmt.Errorf("failed to get first healthy Vizier: %v", err)
		}
	}

	vzClient, err := client.NewVizierClient(ctx, vizierId)

	if err != nil {
		return nil, fmt.Errorf("failed to create Vizier client: %v", err)
	}

	pprof := make(chan string, 1)
	mux := NewPprofMuxer(pprof)
	results, err := vzClient.ExecuteScript(ctx, fmt.Sprintf(pxlTmpl, deploymentName), mux)

	if err != nil {
		return nil, err
	}

	err = results.Stream()

	if err != nil {
		return nil, err
	}
	defer results.Close()

	pprofHex := <-pprof

	// TODO(ddelnano): The pxl script converts the pprof column from raw binary into hex.
	// This necessitates decoding the hex string back into binary. Investigate if we can
	// avoid this and have the pxapi return the pprof binary directly.
	cleaned := strings.ReplaceAll(pprofHex, "\\x", "")
	bytes, err := hex.DecodeString(cleaned)

	if err != nil {
		return nil, fmt.Errorf("failed to decode hex string: %v", err)
	}

	err = os.WriteFile("raw.pprof", bytes, 0644)
	if err != nil {
		log.Fatal(err)
	}
	pprofFile := dag.CurrentModule().WorkdirFile("raw.pprof")
	pprofFile.Export(ctx, "raw.pprof")
	return pprofFile, nil
}

func (m *PixieModule) GetApplicationBinary(
	ctx context.Context,
	gcloudConfig *dagger.Directory,
	kubeconfig *dagger.File,
	deploymentName,
	containerName string,
) (*dagger.File, error) {
	fmt.Println("Getting application binary for deployment:", deploymentName, "and container:", containerName)
	containerImage, err := m.GetImageFromDeployment(ctx, gcloudConfig, kubeconfig, deploymentName, containerName)
	fmt.Println("Container image: ", containerImage)

	if err != nil {
		fmt.Println("Error getting container image:", err)
		return nil, fmt.Errorf("failed to get container image from deployment: %v", err)
	}

	entrypoints, err := dag.Container().
		From(containerImage).
		Entrypoint(ctx)

	if err != nil {
		return nil, fmt.Errorf("failed to get entrypoint: %w", err)
	}

	if len(entrypoints) != 1 {
		return nil, fmt.Errorf("expected exactly one entrypoint, received %v", entrypoints)
	}

	return dag.Container().
		From(containerImage).
		File(entrypoints[0]), nil
}

func (m *PixieModule) CreatePgoProfile(
	ctx context.Context,
	gcloudConfig *dagger.Directory,
	kubeconfig *dagger.File,
	// +default="getcosmic.ai:443"
	cloudAddr,
	// +default=""
	vizierId,
	deploymentName,
	// +default=""
	containerName string,
	apiKey *dagger.Secret,
) (*dagger.File, error) {
	pprof, err := m.CollectProfile(ctx, cloudAddr, vizierId, deploymentName, apiKey)
	pprof.Export(ctx, inputFilepath)
	binary, err := m.GetApplicationBinary(ctx, gcloudConfig, kubeconfig, deploymentName, containerName)
	if err != nil {
		log.Fatal(err)
	}
	binary.Export(ctx, goAppBinary)

	fmt.Printf("Processing pprof file: %s\n", inputFilepath)
	_, err = buildinfo.ReadFile(goAppBinary)

	if err != nil {
		// Container binary is not a Go binary, this pprof module is not applicable.
		log.Fatal(err)
	}
	// fmt.Printf("Build info go version: %s\n", info.GoVersion)

	var (
		textStart = uint64(0)

		symtab  []byte
		pclntab []byte
	)

	file, err := os.Open(inputFilepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Parse the pprof file
	p, err := profile.Parse(file)
	if err != nil {
		log.Fatal(err)
	}
	// your custom logic here
	// Read in the ELF file
	f, err := elf.Open(goAppBinary)
	if err != nil {
		log.Fatal(err)
	}
	if sect := f.Section(".text"); sect != nil {
		textStart = sect.Addr
	}

	sectionName := ".gosymtab"
	sect := f.Section(".gosymtab")
	if sect == nil {
		// try .data.rel.ro.gosymtab, for PIE binaries
		sectionName = ".data.rel.ro.gosymtab"
		sect = f.Section(".data.rel.ro.gosymtab")
	}
	if sect != nil {
		if symtab, err = sect.Data(); err != nil {
			log.Fatal("read %s section: %w", sectionName, err)
		}
	} else {
		// if both sections failed, try the symbol
		// symtab = symbolData(f, "runtime.symtab", "runtime.esymtab")
	}

	sectionName = ".gopclntab"
	sect = f.Section(".gopclntab")
	if sect == nil {
		// try .data.rel.ro.gopclntab, for PIE binaries
		sectionName = ".data.rel.ro.gopclntab"
		sect = f.Section(".data.rel.ro.gopclntab")
	}
	if sect != nil {
		if pclntab, err = sect.Data(); err != nil {
			log.Fatal("read %s section: %w", sectionName, err)
		}
	} else {
		// if both sections failed, try the symbol
		// pclntab = symbolData(f, "runtime.pclntab", "runtime.epclntab")
	}

	runtimeTextAddr, ok := runtimeTextAddr(f)
	if ok {
		textStart = runtimeTextAddr
	}

	tab, err := gosym.NewTable(symtab, gosym.NewLineTable(pclntab, textStart))

	for _, f := range p.Function {

		// Skip functions that are not relevant for PGO or are not Go functions.
		// [k] indicates kernel functions, [m] indicates shared library code, and 0x indicates
		// addresses that weren't symbolized.

		if strings.HasPrefix(f.Name, "[k] ") ||
			strings.HasPrefix(f.Name, "[m] ") ||
			strings.HasPrefix(f.Name, "0x") ||

			// Pixie's ELF based symbolizer recognizes these symbols but they don't
			// exist in the .pclntab section, so we skip them. If/when Pixie has a .pclntab
			// based symbolizer these symbols won't exist.
			strings.HasPrefix(f.Name, "local.") {
			continue
		}

		fnLookup := tab.LookupFunc(f.Name)
		// This case occurs when Pixie's symbolization, which is ELF based, fails to match what is expected in the
		// .pclntab section.
		if fnLookup == nil {
			fmt.Printf("Function %s not found in symbol table\n", f.Name)
		} else {
			file, line, _ := tab.PCToLine(fnLookup.Entry)
			fmt.Printf("file: %s line: %d\n", file, line)

			// Ensure Function.StartLine is set to satisfy the PGO requirements
			// https://go.dev/doc/pgo#alternative-sources
			//
			// Copied below is the start_line requirement:
			//
			//  > Function.start_line must be set. This is the line number of the start
			//  > of the function. i.e., the line containing the func keyword. The Go compiler
			//  > uses this field to compute line offsets of samples
			//  > (Location.Line.line - Function.start_line). Note that many existing pprof
			//  > converters omit this field."

			f.StartLine = int64(line)
		}
	}

	processed, err := os.OpenFile(outputFilepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	err = p.WriteUncompressed(processed)
	if err != nil {
		log.Fatal(err)
	}
	processedPprof := dag.CurrentModule().WorkdirFile(outputFilepath)
	processedPprof.Export(ctx, outputFilepath)
	return processedPprof, nil
}

func runtimeTextAddr(f *elf.File) (uint64, bool) {
	elfSyms, err := f.Symbols()
	if err != nil {
		return 0, false
	}

	for _, s := range elfSyms {
		if s.Name != "runtime.text" {
			continue
		}

		return s.Value, true
	}

	return 0, false
}
