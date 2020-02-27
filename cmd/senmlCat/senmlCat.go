package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/cisco/senml"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/pprof"
)

var doIndentPtr = flag.Bool("i", false, "indent output")
var doPrintPtr = flag.Bool("print", false, "print output to stdout")
var doResolvePtr = flag.Bool("resolve", false, "resolve SenML records")
var postUrl = flag.String("post", "", "URL to HTTP POST output to")
var topic = flag.String("topic", "senml", "Apache Kafka topic or InfluxDB series name ")

var doJsonPtr = flag.Bool("json", false, "output JSON formatted SenML ")
var doCborPtr = flag.Bool("cbor", false, "output CBOR formatted SenML ")
var doXmlPtr = flag.Bool("xml", false, "output XML formatted SenML ")
var doCsvPtr = flag.Bool("csv", false, "output CSV formatted SenML ")
var doMpackPtr = flag.Bool("mpack", false, "output MessagePack formatted SenML ")
var doLinpPtr = flag.Bool("linp", false, "output InfluxDB LineProtcol formatted SenML ")
var doJsonLinePtr = flag.Bool("jsonl", false, "outpute JSON formatted SenML Record lines")

var doIJsonStreamPtr = flag.Bool("ijson", false, "input JSON formatted SenML")
var doIJsonLinePtr = flag.Bool("ijsonl", false, "input JSON formatted SenML Record lines")
var doIXmlPtr = flag.Bool("ixml", false, "input XML formatted SenML ")
var doICborPtr = flag.Bool("icbor", false, "input CBOR formatted SenML ")
var doIMpackPtr = flag.Bool("impack", false, "input MessagePack formatted SenML ")

func decodeTimed(msg []byte) (senml.SenML, error) {
	var s senml.SenML
	var err error

	var format senml.Format = senml.JSON
	switch {
	case *doIJsonStreamPtr:
		format = senml.JSON
	case *doIJsonLinePtr:
		format = senml.JSONLINE
	case *doICborPtr:
		format = senml.CBOR
	case *doIXmlPtr:
		format = senml.XML
	case *doIMpackPtr:
		format = senml.MPACK
	}

	s, err = senml.Decode(msg, format)

	return s, err
}

func outputData(data []byte) error {
	if *doPrintPtr {
		fmt.Print(string(data))
	}

	if len(*postUrl) != 0 {
		fmt.Println("PostURL=<" + string(*postUrl) + ">")
		buffer := bytes.NewBuffer(data)
		resp, err := http.Post(string(*postUrl), "application/senml+json", buffer)
		if err != nil {
			fmt.Println("Post to", string(*postUrl), " got error", err.Error())
			return err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("error reading response body: %v", err)
			return err
		}
		if resp.StatusCode != 204 {
			fmt.Println("Post got status ", resp.Status)
			fmt.Println("Post got", string(body))
			return errors.New("WebServer returned: " + string(body))
		}
	}

	return nil
}

func processData(dataIn []byte) error {
	var s senml.SenML
	var err error

	s, err = decodeTimed(dataIn)
	if err != nil {
		fmt.Println("Decode of SenML failed")
		return err
	}

	//fmt.Println( "Senml:", senml.Records )
	if *doResolvePtr {
		s = senml.Normalize(s)
	}

	var dataOut []byte
	options := senml.OutputOptions{}
	if *doIndentPtr {
		options.PrettyPrint = *doIndentPtr
	}
	options.Topic = string(*topic)
	var format senml.Format = senml.JSON
	switch {
	case *doJsonPtr:
		format = senml.JSON
	case *doJsonLinePtr:
		format = senml.JSONLINE
	case *doCborPtr:
		format = senml.CBOR
	case *doXmlPtr:
		format = senml.XML
	case *doCsvPtr:
		format = senml.CSV
	case *doMpackPtr:
		format = senml.MPACK
	case *doLinpPtr:
		format = senml.LINEP
	}
	dataOut, err = senml.Encode(s, format, options)
	if err != nil {
		fmt.Println("Encode of SenML failed")
		return err
	}

	if false {
		reader := bytes.NewReader(dataOut)
		lines := bufio.NewScanner(reader)
		for lines.Scan() {
			err = outputData([]byte(lines.Text()))
			if err != nil {
				fmt.Println("Output of SenML failed:", err)
				return err
			}
		}

		err = lines.Err()
		if err != nil {
			fmt.Println("Encode scanning lines in output")
		}
	} else {
		err = outputData(dataOut)
		if err != nil {
			fmt.Println("Output of SenML failed:", err)
			return err
		}
	}

	return nil
}

func main() {
	var err error

	if false {
		f, err := os.Create("senmlCat.prof")
		if err != nil {
			fmt.Println("error opening profile file", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	flag.Parse()

	//fmt.Print("Reading file ...")
	// load the input
	msg, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		fmt.Println("error reading SenML file", err)
		os.Exit(1)
	}
	//fmt.Println("Done")

	err = processData(msg)
	if err != nil {
		fmt.Println("error processing SenML file", err)
		os.Exit(1)
	}
}
