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
	"fmt"
	"log"
	"os"

	"github.com/google/pprof/profile"
)

const (
	goAppBinary    = "application_binary"
	outputFilepath = "output.pprof"
	containerImage = "gcr.io/pixie-oss/pixie-prod/vizier-cloud_connector_server_image:0.14.15"
)

// TODO(ddelnano): Rename module
type HelloDagger struct{}

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
