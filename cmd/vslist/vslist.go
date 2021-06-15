// vslist is a tool to automate the creation of string value list for types that satisfy the
// fmt.Stringer interface. Given the name of any type that satisfy the fmt.Stringer interface,
// vslist will create a global string list containing all the variables declared in the given dir.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/packages"
)

var (
	typeNames = flag.String("type", "", "comma-separated list of type names; must be set")
	debug     = flag.Bool("debug", false, "debug mode")
	dir       = flag.String("dir", ".", "directories to read and write files")
)

// Generator holds the state of the analysis. Primarily used to buffer
// the output for format.Source.
type Generator struct {
	cbuf bytes.Buffer // Accumulated output for common part.
	tbuf bytes.Buffer // Accumulated output for each type part.
	pkg  *Package     // Package we are scanning.
}

type Package struct {
	name  string
	defs  map[*ast.Ident]types.Object
	files []*File
}

// File holds a single parsed file and associated data.
type File struct {
	pkg  *Package  // Package to which this file belongs.
	file *ast.File // Parsed AST.
	// These fields are reset for each type being generated.
	typeName      string   // Name of the variable type.
	variableNames []string // Variable names defined.
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Llongfile)
	if !*debug {
		log.SetFlags(0)
		log.SetPrefix("vslist: ")
	}
	if len(*typeNames) == 0 {
		log.Fatal("typeNames must be set")
	}

	types := strings.Split(*typeNames, ",")

	g := Generator{}
	g.parsePackage()
	g.WriteCbuf("// Code generated by \"github.com/Sudalight/tools/cmd/vslist\"; DO NOT EDIT.\n")
	g.WriteCbuf("// If something went wrong, re-run the \"vslist\" command to generate the file again.\n\n")

	g.WriteCbuf("package %s\n\n", g.pkg.name)

	for i := range types {
		g.refreshTbuf()
		g.generate(types[i])
		// Format the output.
		src := g.format()

		// Write to file.
		baseName := fmt.Sprintf("%s_string_list.go", strings.ToLower(types[i]))
		outputName := filepath.Join(*dir, strings.ToLower(baseName))
		err := ioutil.WriteFile(outputName, src, 0644)
		if err != nil {
			log.Fatalf("writing output: %s", err)
		}
	}
}

func (g *Generator) generate(typeName string) {
	for i := range g.pkg.files {
		file := g.pkg.files[i]
		file.typeName = typeName
		ast.Inspect(file.file, file.genDecl)

		if len(file.variableNames) == 0 {
			continue
		}
		g.WriteTbuf("var %sStringList = []string{\n", file.typeName)
		for j := range file.variableNames {
			g.WriteTbuf("%s.String(),\n", file.variableNames[j])
		}
		g.WriteTbuf("}\n")
	}
}

// parsePackage analyzes the single package constructed from the patterns and tags.
// parsePackage exits if there is an error.
func (g *Generator) parsePackage() {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles |
			packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedTypes | packages.NeedTypesSizes |
			packages.NeedSyntax | packages.NeedTypesInfo,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, *dir)
	if err != nil {
		log.Fatal(err)
	}
	if len(pkgs) != 1 {
		log.Fatalf("error: %d packages found", len(pkgs))
	}
	if pkgs[0].Name == "" {
		log.Fatalf("no go package found in dir:%s", *dir)
	}
	g.addPackage(pkgs[0])
}

// addPackage adds a type checked Package and its syntax files to the generator.
func (g *Generator) addPackage(pkg *packages.Package) {
	g.pkg = &Package{
		name:  pkg.Name,
		defs:  pkg.TypesInfo.Defs,
		files: make([]*File, len(pkg.Syntax)),
	}

	for i, file := range pkg.Syntax {
		g.pkg.files[i] = &File{
			file: file,
			pkg:  g.pkg,
		}
	}
}

func (f *File) genDecl(node ast.Node) bool {
	decl, ok := node.(*ast.GenDecl)
	if !ok || decl.Tok != token.VAR {
		// We only care about VAR declarations.
		return true
	}
	for _, spec := range decl.Specs {
		vspec := spec.(*ast.ValueSpec) // Guaranteed to succeed as this is VAR.
		typ := ""
		if vspec.Type == nil && len(vspec.Values) > 0 {
			ce, ok := vspec.Values[0].(*ast.CompositeLit)
			if !ok {
				continue
			}

			id, ok := ce.Type.(*ast.Ident)
			if !ok {
				continue
			}
			typ = id.Name
		} else {
			ident, ok := vspec.Type.(*ast.Ident)
			if !ok {
				continue
			}
			typ = ident.Name
		}
		if typ != f.typeName {
			continue
		}
		for i := range vspec.Names {
			f.variableNames = append(f.variableNames, vspec.Names[i].Name)
		}
	}
	return false
}

func (g *Generator) WriteTbuf(format string, args ...interface{}) {
	fmt.Fprintf(&g.tbuf, format, args...)
}

func (g *Generator) WriteCbuf(format string, args ...interface{}) {
	fmt.Fprintf(&g.cbuf, format, args...)
}

// format returns the gofmt-ed contents of the Generator's buffer.
func (g *Generator) format() []byte {
	buf := append(g.cbuf.Bytes(), g.tbuf.Bytes()...)
	src, err := format.Source(buf)
	if err != nil {
		// Should never happen, but can arise when developing this code.
		// The user can compile the output to see the error.
		log.Printf("warning: internal error: invalid Go generated: %s", err)
		log.Printf("warning: compile the package to analyze the error")
		return buf
	}
	return src
}

func (g *Generator) refreshTbuf() {
	g.tbuf = bytes.Buffer{}
	for i := range g.pkg.files {
		g.pkg.files[i].variableNames = make([]string, 0)
	}
}
