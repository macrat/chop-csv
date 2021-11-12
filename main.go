package main

import (
	"crypto/md5"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dsnet/compress/bzip2"
	"golang.org/x/text/encoding/japanese"
)

var (
	version = "0.1.0"

	dateFormat = flag.String("date-format", "20060102", "Date format of the first column. See also https://pkg.go.dev/time#pkg-constants")
	outputDir  = flag.String("out-dir", "chopped", "The output directory.")
	utf8Mode   = flag.Bool("utf8", false, "Enable UTF-8 decoding. In default, decode as Shift-JIS.")
)

func md5sum(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

// Writer is a compressed CSV writer.
//
// WARNING: this struct reads commandline flags directly.
type Writer struct {
	f *os.File
	b *bzip2.Writer
	c *csv.Writer
}

func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	b, err := bzip2.NewWriter(f, &bzip2.WriterConfig{
		Level: bzip2.BestCompression,
	})
	if err != nil {
		return nil, err
	}

	c := csv.NewWriter(b)

	return &Writer{f, b, c}, nil
}

func (w *Writer) Close() error {
	if w == nil {
		return nil
	}

	w.c.Flush()
	if err := w.b.Close(); err != nil {
		return err
	}
	return w.f.Close()
}

func (w *Writer) Write(record []string) error {
	return w.c.Write(record)
}

func (w *Writer) Name() string {
	if w == nil {
		return ""
	}
	return w.f.Name()
}

// Reader is a CSV reader.
//
// WARNING: this struct reads commandline flags directly.
type Reader struct {
	f *os.File
	c *csv.Reader
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var r io.Reader = f
	if !*utf8Mode {
		r = japanese.ShiftJIS.NewDecoder().Reader(f)
	}

	return &Reader{f, csv.NewReader(r)}, nil
}

func (r *Reader) Close() {
	r.f.Close()
}

func (r *Reader) Read() ([]string, error) {
	return r.c.Read()
}

// Chop chops input file.
//
// WARNING: this method can stop program with log.Fatal.
func Chop(inputPath string) {
	log.Printf("open input file: %s", inputPath)

	r, err := Open(inputPath)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer r.Close()

	abs, err := filepath.Abs(inputPath)
	if err != nil {
		log.Fatalf("failed to resolve input file path: %s", err)
	}
	csvName := fmt.Sprintf("%s.csv.bz2", md5sum(abs))

	var w *Writer

	for line := 0; ; line++ {
		row, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			w.Close()
			log.Fatal(err)
		}

		t, err := time.Parse(*dateFormat, row[0])
		if err != nil {
			log.Printf("ignore row %d because invalid timestamp: %s: %s", line+1, row[0], err)
			continue
		}

		fpath := filepath.Join(*outputDir, filepath.FromSlash(t.Format("year=2006/month=1/day=2")))
		fname := filepath.Join(fpath, csvName)
		if w.Name() != fname {
			if w == nil {
				w.Close()
			}

			log.Printf("write to %s", fname)
			os.MkdirAll(fpath, 0755)

			w, err = Create(fname)
			if err != nil {
				log.Fatal(err)
			}
		}

		w.Write(row)
	}

	w.Close()
}

// ChopRecursive is a directory recursive version of Chop function.
func ChopRecursive(inputPath string) {
	s, err := os.Stat(inputPath)
	if err != nil {
		log.Fatalf("failed to get file information: %s", err)
	}

	if !s.IsDir() {
		Chop(inputPath)
		return
	}

	log.Print("search CSV files from %s", inputPath)

	err = filepath.Walk(inputPath, func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() && filepath.Ext(path) == ".csv" {
			Chop(path)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Println("Usage: chop-csv [OPTIONS] help|version|FILE...")
		fmt.Println()
		fmt.Println("OPTIONS:")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if flag.Arg(0) == "version" {
		fmt.Printf("chop-csv %s\n", version)
		return
	}
	if flag.Arg(0) == "help" {
		flag.Usage()
		return
	}

	for _, f := range flag.Args() {
		ChopRecursive(f)
	}
}
