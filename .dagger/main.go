// A generated module for HelloDagger functions
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
	"strings"

	"github.com/google/pprof/profile"
	"px.dev/pxapi"
	"px.dev/pxapi/types"
)

const (
	goAppBinary    = "application_binary"
	outputFilepath = "output.pprof"
	containerImage = "gcr.io/pixie-oss/pixie-prod/vizier-cloud_connector_server_image:0.14.15"
	pxlTmpl        = `
import px

stack_traces = px.DataFrame(table='stack_traces.beta', start_time='-5m')
stack_traces.deployment = stack_traces.ctx['deployment']
# Filter out stack traces to just the go service we are trying to deploy
stack_traces = stack_traces[stack_traces.deployment == '%s']
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

// TODO(ddelnano): Rename module
type HelloDagger struct{}

func (m *HelloDagger) CollectProfile(
	ctx context.Context,
	// +default="getcosmic.ai:443"
	cloudAddr,
	vizierId,
	deploymentName string,
	apiKey *dagger.Secret,
) (*dagger.File, error) {
	fmt.Println("CollectProfile called with pxCloudAddr:", cloudAddr, "and deploymentName:", deploymentName)
	apiKeyPlaintext, err := apiKey.Plaintext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get plaintext API key: %v", err)
	}

	client, err := pxapi.NewClient(ctx, pxapi.WithCloudAddr(cloudAddr), pxapi.WithAPIKey(apiKeyPlaintext))
	if err != nil {
		return nil, fmt.Errorf("failed to create Pixie API client: %v", err)
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

	// TODO(ddelnano): The pxl script converts the pprof binary into hex, resulting in
	// this Go code needing to decode the hex string. Investigate if we can avoid this
	// and have the pxapi return the pprof binary directly.
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

func (m *HelloDagger) GetApplicationBinary(ctx context.Context, containerImage string) (*dagger.File, error) {
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

// TODO(ddelnano): Remove CopyFile to something more meaningful
func (m *HelloDagger) CopyFile(ctx context.Context, pprof *dagger.File) (*dagger.File, error) {
	pprof.Export(ctx, "input.pprof")
	binary, err := m.GetApplicationBinary(ctx, containerImage)
	if err != nil {
		log.Fatal(err)
	}
	binary.Export(ctx, goAppBinary)

	info, err := buildinfo.ReadFile(goAppBinary)

	if err != nil {
		// Container binary is not a Go binary, this pprof module is not applicable.
		log.Fatal(err)
	}
	fmt.Printf("Build info go version: %s\n", info.GoVersion)

	var (
		textStart = uint64(0)

		symtab  []byte
		pclntab []byte
	)

	file, err := os.Open("input.pprof")
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
		fnLookup := tab.LookupFunc(f.Name)
		// This case happens when Pixie's symbolization fails to match what is expected in the
		// .pclntab section. An example of this is kernel stack frames: "[k] do_sock_setsockopt"
		// This shouldn't affect PGO.
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
